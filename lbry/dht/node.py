import logging
import asyncio
import typing
import socket
import ipaddress
from collections import defaultdict

from prometheus_client import Gauge

from lbry.utils import aclosing, resolve_host
from lbry.dht import constants
from lbry.dht.peer import make_kademlia_peer, PeerManager
from lbry.dht.protocol.distance import Distance
from lbry.dht.protocol.iterative_find import IterativeNodeFinder, IterativeValueFinder, InterleavingFinder
from lbry.dht.protocol.protocol import KademliaProtocol

if typing.TYPE_CHECKING:
    from lbry.dht.peer import KademliaPeer

log = logging.getLogger(__name__)


class Node:
    storing_peers_metric = Gauge(
        "storing_peers", "Number of peers storing blobs announced to this node", namespace="dht_node",
        labelnames=("scope",),
    )
    stored_blob_with_x_bytes_colliding = Gauge(
        "stored_blobs_x_bytes_colliding", "Number of blobs with at least X bytes colliding with this node id prefix",
        namespace="dht_node", labelnames=("amount",)
    )
    def __init__(self, loop: asyncio.AbstractEventLoop, peer_manager: 'PeerManager', node_id: bytes, udp_port: int,
                 internal_udp_port: int, peer_port: int, external_ip: typing.List[str],
                 rpc_timeout: float = constants.RPC_TIMEOUT,
                 split_buckets_under_index: int = constants.SPLIT_BUCKETS_UNDER_INDEX, is_bootstrap_node: bool = False,
                 storage: typing.Optional['SQLiteStorage'] = None):
        self.loop = loop
        self.node_id = node_id
        self.internal_udp_port = internal_udp_port
        self.protocols: typing.Dict[str, KademliaProtocol] = {}
        if not isinstance(external_ip, (list, dict)):
            external_ip = [external_ip]
        if not isinstance(external_ip, dict):
            external_ip = {ip: ip for ip in external_ip}
        i = 0
        for local, external in external_ip.items():
            # HACK: Modify final byte of node ID to make an ID unique for each external IP.
            pid = node_id[:-1] + bytes([i])
            i += 1
            #print(f'node: {pid.hex()}: external: {external}')
            # Create separate peer manager for each protocol, as we don't
            # want them interfering.
            # TODO: Eliminate 'peer_manager' argument, which is being ignored.
            peer_manager = PeerManager(loop)
            self.protocols.update({
                local:
                KademliaProtocol(loop, peer_manager, pid, external, udp_port, peer_port, rpc_timeout,
                                 split_buckets_under_index, is_bootstrap_node)
            })
        self.listening_ports: typing.List[asyncio.DatagramTransport] = []
        self.joined = asyncio.Event()
        self._join_task: asyncio.Task = None
        self._refresh_tasks: typing.List[asyncio.Task] = []
        self._storage = storage

    @property
    def stored_blob_hashes(self):
        # TODO: Multiple protocols.
        for protocol in self.protocols.values():
            return protocol.data_store.keys()

    async def _refresh_protocol(self, protocol, force_once=False):
        while True:
            # remove peers with expired blob announcements from the datastore
            protocol.data_store.removed_expired_peers()

            total_peers: typing.List['KademliaPeer'] = []
            # add all peers in the routing table
            total_peers.extend(protocol.routing_table.get_peers())
            # add all the peers who have announced blobs to us
            storing_peers = protocol.data_store.get_storing_contacts()
            self.storing_peers_metric.labels("global").set(len(storing_peers))
            total_peers.extend(storing_peers)

            counts = {0: 0, 1: 0, 2: 0}
            node_id = protocol.node_id
            for blob_hash in protocol.data_store.keys():
                bytes_colliding = 0 if blob_hash[0] != node_id[0] else 2 if blob_hash[1] == node_id[1] else 1
                counts[bytes_colliding] += 1
            self.stored_blob_with_x_bytes_colliding.labels(amount=0).set(counts[0])
            self.stored_blob_with_x_bytes_colliding.labels(amount=1).set(counts[1])
            self.stored_blob_with_x_bytes_colliding.labels(amount=2).set(counts[2])

            # get ids falling in the midpoint of each bucket that hasn't been recently updated
            node_ids = protocol.routing_table.get_refresh_list(0, True)

            if protocol.routing_table.get_peers():
                # if we have node ids to look up, perform the iterative search until we have k results
                while node_ids:
                    peers = await self.peer_search(node_ids.pop())
                    total_peers.extend(peers)
            else:
                if force_once:
                    break
                fut = asyncio.Future()
                self.loop.call_later(constants.REFRESH_INTERVAL // 4, fut.set_result, None)
                await fut
                continue

            # ping the set of peers; upon success/failure the routing able and last replied/failed time will be updated
            to_ping = [peer for peer in set(total_peers) if protocol.peer_manager.peer_is_good(peer) is not True]
            if to_ping:
                protocol.ping_queue.enqueue_maybe_ping(*to_ping, delay=0)
            if self._storage:
                await self._storage.save_kademlia_peers(protocol.routing_table.get_peers(), node_id=protocol.node_id)
            if force_once:
                break

            fut = asyncio.Future()
            self.loop.call_later(constants.REFRESH_INTERVAL, fut.set_result, None)
            await fut

    async def refresh_node(self, force_once=False):
        for _, protocol in self.protocols.items():
            await self._refresh_protocol(protocol, force_once)

    async def _announce_blob(self, protocol, blob_hash: str) -> typing.List[bytes]:
        hash_value = bytes.fromhex(blob_hash)
        assert len(hash_value) == constants.HASH_LENGTH
        peers = await self.peer_search(hash_value)

        if not protocol.external_ip:
            raise Exception("Cannot determine external IP")
        log.debug("Store to %i peers", len(peers))
        for peer in peers:
            log.debug("store to %s %s %s", peer.address, peer.udp_port, peer.tcp_port)
        stored_to_tup = await asyncio.gather(
            *(protocol.store_to_peer(hash_value, peer) for peer in peers)
        )
        stored_to = [node_id for node_id, contacted in stored_to_tup if contacted]
        if stored_to:
            log.debug(
                "Stored %s to %i of %i attempted peers", hash_value.hex()[:8],
                len(stored_to), len(peers)
            )
        else:
            log.debug("Failed announcing %s, stored to 0 peers", blob_hash[:8])
        return stored_to

    async def announce_blob(self, blob_hash: str) -> typing.List[bytes]:
        stored_to = []
        for _, protocol in self.protocols.items():
            stored_to.extend(await self._announce_blob(protocol, blob_hash))
        return stored_to

    def stop(self) -> None:
        if self.joined.is_set():
            self.joined.clear()
        if self._join_task:
            self._join_task.cancel()
        for task in self._refresh_tasks:
            if not (task.done() or task.cancelled()):
                task.cancel()
        for _, protocol in self.protocols.items():
            if protocol.ping_queue.running:
                protocol.ping_queue.stop()
                protocol.stop()
        for port in self.listening_ports:
            port.close()
        self._join_task = None
        self.listening_ports.clear()
        log.info("Stopped DHT node")

    async def start_listening(self, interface: typing.Optional[str] = None, family: int = socket.AF_UNSPEC) -> None:
        if interface not in self.protocols:
            raise ValueError(f'unrecognized local interface {interface}')
        protocol = self.protocols[interface]
        if protocol.transport:
            log.warning("Already bound to port %s", protocol.transport.get_extra_info('sockname')[:2])

        ipaddr = ipaddress.ip_address(interface)
        if ipaddr.version == 4:
            port, _ = await self.loop.create_datagram_endpoint(
                lambda: protocol,
                (ipaddr.compressed, self.internal_udp_port),
                family=socket.AF_INET,
            )
        elif ipaddr.version == 6:
            # Because dualstack / IPv4 mapped address behavior on an IPv6 socket
            # differs based on system config, create the socket with IPV6_V6ONLY.
            # This disables the IPv4 mapped feature, so we don't need to consider
            # when an IPv6 socket may interfere with IPv4 binding / traffic.
            sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
            sock.bind((ipaddr.compressed, self.internal_udp_port))
            port, _ = await self.loop.create_datagram_endpoint(lambda: protocol, sock=sock)
        else:
            raise ValueError(f'unexpected IP address version {ipaddr.version}')
        log.warning("DHT node listening on UDP %s", port.get_extra_info('sockname')[:2])
        self.listening_ports.append(port)

    async def join_network(self, interfaces: typing.Optional[typing.List[str]] = None,
                           known_node_urls: typing.Optional[typing.List[typing.Tuple[str, int]]] = None):
        def peers_from_urls(protocol, urls: typing.Optional[typing.List[typing.Tuple[bytes, str, int, int]]]):
            peer_addresses = []
            for node_id, address, udp_port, tcp_port in urls:
                if (node_id, address, udp_port, tcp_port) not in peer_addresses and \
                        (address, udp_port) != (protocol.external_ip, protocol.udp_port):
                    peer_addresses.append((node_id, address, udp_port, tcp_port))
            return [make_kademlia_peer(*peer_address) for peer_address in peer_addresses]

        if not isinstance(interfaces, list):
            interfaces = [interfaces]
        try:
            for interface in interfaces:
                addr = None
                if not interface:
                    resolved = ['::', '0.0.0.0'] # unspecified address
                else:
                    resolved = await resolve_host(
                        interface, self.internal_udp_port, 'udp',
                        family=socket.AF_UNSPEC, all_results=True
                    )
                for addr in resolved:
                    await self.start_listening(addr)
        except Exception as e:
            if not isinstance(e, asyncio.CancelledError):
                log.error("DHT node failed to listen on (%s:%i) : %s", addr or interface, self.internal_udp_port, e)
            self.stop()
            raise

        for _, protocol in self.protocols.items():
            if not protocol.transport:
                continue
            protocol.start()
            protocol.ping_queue.start()
            self._refresh_tasks.append(self.loop.create_task(self._refresh_protocol(protocol)))

        async def _update_protocol(protocol):
            print(f"update protocol {protocol.transport.get_extra_info('sockname')[:2]}")
            if protocol.routing_table.get_peers():
                return 1, []
            else:
                if self.joined.is_set():
                    self.joined.clear()
                seed_peers = peers_from_urls(
                    protocol,
                    await self._storage.get_persisted_kademlia_peers(node_id=protocol.node_id)
                ) if self._storage else []
                if not seed_peers:
                    try:
                        seed_peers.extend(peers_from_urls(
                            protocol,
                            [
                                (None, addr, udp_port, None)
                                for host, udp_port in known_node_urls or []
                                for addr in await resolve_host(
                                    host, udp_port, 'udp',
                                    family=protocol.external_ip_family,
                                    all_results=True
                                )
                            ]
                        ))
                    except socket.gaierror:
                        return 30, []

                protocol.peer_manager.reset()
                protocol.ping_queue.enqueue_maybe_ping(*seed_peers, delay=0.0)
                return 1, seed_peers

        while True:
            delay = 0
            seed_peers = []
            for _, protocol in self.protocols.items():
                try:
                    d, peers = await _update_protocol(protocol)
                    delay = max(delay, d)
                    seed_peers.extend(peers)
                except Exception:
                    log.exception("update_protocol exception raised")
                    raise
            if seed_peers:
                await self.peer_search(self.node_id, shortlist=seed_peers, count=32)
            if all(map(lambda protocol: protocol.routing_table.get_peers(), self.protocols.values())):
                if not self.joined.is_set():
                    self.joined.set()
                    log.warning(
                        "joined dht, %i peers known in %i buckets", len(protocol.routing_table.get_peers()),
                        protocol.routing_table.buckets_with_contacts()
                    )
            await asyncio.sleep(delay)

    def start(self, interface: typing.List[str],
              known_node_urls: typing.Optional[typing.List[typing.Tuple[str, int]]] = None):
        self._join_task = self.loop.create_task(self.join_network(interface, known_node_urls))

    def get_iterative_node_finder(self, key: bytes, shortlist: typing.Optional[typing.List['KademliaPeer']] = None,
                                  max_results: int = constants.K) -> InterleavingFinder[IterativeNodeFinder]:
        finders = []
        compatible_peers = defaultdict(list)
        for peer in shortlist or []:
            compatible_peers[ipaddress.ip_address(peer.address).version].append(peer)
        for protocol in self.protocols.values():
            shortlist = compatible_peers[protocol.external_ip_version] or protocol.routing_table.find_close_peers(key)
            finders.append(IterativeNodeFinder(self.loop, protocol, key, max_results, shortlist))
        return InterleavingFinder(*finders)

    def get_iterative_value_finder(self, key: bytes, shortlist: typing.Optional[typing.List['KademliaPeer']] = None,
                                   max_results: int = -1) -> InterleavingFinder[IterativeValueFinder]:
        finders = []
        compatible_peers = defaultdict(list)
        for peer in shortlist or []:
            compatible_peers[ipaddress.ip_address(peer.address).version].append(peer)
        for protocol in self.protocols.values():
            shortlist = compatible_peers[protocol.external_ip_version] or protocol.routing_table.find_close_peers(key)
            finders.append(IterativeValueFinder(self.loop, protocol, key, max_results, shortlist))
        return InterleavingFinder(*finders)

    async def peer_search(self, node_id: bytes, count=constants.K, max_results=constants.K * 2,
                          shortlist: typing.Optional[typing.List['KademliaPeer']] = None
                          ) -> typing.List['KademliaPeer']:
        peers = []
        async with aclosing(self.get_iterative_node_finder(
                node_id, shortlist=shortlist, max_results=max_results)) as node_finder:
            async for iteration_peers in node_finder:
                peers.extend(iteration_peers)
        distance = Distance(node_id)
        peers.sort(key=lambda peer: distance(peer.node_id))
        return peers[:count]

    def contains_peer(self, peer: 'KademliaPeer'):
        peer.node_id in [p.node_id for p in self.protocols.values()] or \
            peer.address in [p.external_ip for p in self.protocols.values()]

    async def _accumulate_peers_for_value(self, search_queue: asyncio.Queue, result_queue: asyncio.Queue):
        tasks = []
        try:
            while True:
                blob_hash = await search_queue.get()
                tasks.append(self.loop.create_task(self._peers_for_value_producer(blob_hash, result_queue)))
        finally:
            for task in tasks:
                task.cancel()

    async def _peers_for_value_producer(self, blob_hash: str, result_queue: asyncio.Queue):
        async def put_into_result_queue_after_pong(_peer):
            protocol = None
            for proto in self.protocols.values():
                if proto.external_ip_version == ipaddress.ip_address(_peer.address).version:
                    protocol = proto
                    break
            if not protocol:
                log.error("no protocol to contact peer %s:%i for %s", _peer.address, _peer.udp_port, blob_hash)
                return
            try:
                await protocol.get_rpc_peer(_peer).ping()
                result_queue.put_nowait([_peer])
                log.debug("pong from %s:%i for %s", _peer.address, _peer.udp_port, blob_hash)
            except asyncio.TimeoutError:
                pass

        # prioritize peers who reply to a dht ping first
        # this minimizes attempting to make tcp connections that won't work later to dead or unreachable peers
        async with aclosing(self.get_iterative_value_finder(bytes.fromhex(blob_hash))) as value_finder:
            async for results in value_finder:
                to_put = []
                for peer in results:
                    if self.contains_peer(peer):
                        continue
                    is_good = any(p.peer_manager.peer_is_good(peer) for p in self.protocols.values())
                    if is_good:
                        # the peer has replied recently over UDP, it can probably be reached on the TCP port
                        to_put.append(peer)
                    elif is_good is None:
                        if not peer.udp_port:
                            # TODO: use the same port for TCP and UDP
                            # the udp port must be guessed
                            # default to the ports being the same. if the TCP port appears to be <=0.48.0 default,
                            # including on a network with several nodes, then assume the udp port is proportionately
                            # based on a starting port of 4444
                            udp_port_to_try = peer.tcp_port
                            if 3400 > peer.tcp_port > 3332:
                                udp_port_to_try = (peer.tcp_port - 3333) + 4444
                            self.loop.create_task(put_into_result_queue_after_pong(
                                make_kademlia_peer(peer.node_id, peer.address, udp_port_to_try, peer.tcp_port)
                            ))
                        else:
                            self.loop.create_task(put_into_result_queue_after_pong(peer))
                    else:
                        # the peer is known to be bad/unreachable, skip trying to connect to it over TCP
                        log.debug("skip bad peer %s:%i for %s", peer.address, peer.tcp_port, blob_hash)
                if to_put:
                    result_queue.put_nowait(to_put)

    def accumulate_peers(self, search_queue: asyncio.Queue,
                         peer_queue: typing.Optional[asyncio.Queue] = None
                         ) -> typing.Tuple[asyncio.Queue, asyncio.Task]:
        queue = peer_queue or asyncio.Queue()
        return queue, self.loop.create_task(self._accumulate_peers_for_value(search_queue, queue))


async def get_kademlia_peers_from_hosts(peer_list: typing.List[typing.Tuple[str, int]]) -> typing.List['KademliaPeer']:
    peer_address_list = [(await resolve_host(url, port, proto='tcp'), port) for url, port in peer_list]
    kademlia_peer_list = [make_kademlia_peer(None, address, None, tcp_port=port, allow_localhost=True)
                          for address, port in peer_address_list]
    return kademlia_peer_list
