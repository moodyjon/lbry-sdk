import asyncio
from binascii import hexlify

from lbry.extras.daemon.storage import SQLiteStorage
from lbry.conf import Config
from lbry.dht import constants
from lbry.dht.node import Node
from lbry.dht import peer as dht_peer
from lbry.dht.peer import PeerManager, make_kademlia_peer
from lbry.testcase import AsyncioTestCase


class DHTIntegrationTest(AsyncioTestCase):

    async def asyncSetUp(self):
        dht_peer.ALLOW_LOCALHOST = True
        self.addCleanup(setattr, dht_peer, 'ALLOW_LOCALHOST', False)
        import logging
        logging.getLogger('asyncio').setLevel(logging.ERROR)
        logging.getLogger('lbry.dht').setLevel(logging.WARN)
        self.nodes = []
        self.known_node_addresses = []

    async def create_node(self, node_id, port, external_ip=['::1', '127.0.0.1']):
        storage = SQLiteStorage(Config(), ":memory:", self.loop, self.loop.time)
        await storage.open()
        node = Node(self.loop, PeerManager(self.loop), node_id=node_id,
                    udp_port=port, internal_udp_port=port,
                    peer_port=3333, external_ip=external_ip,
                    storage=storage)
        self.addCleanup(node.stop)
        for protocol in node.protocols.values():
            protocol.rpc_timeout = .5
            protocol.ping_queue._default_delay = .5
        return node

    async def setup_network(self, size: int, start_port=40000, seed_nodes=1, external_ip=['::1', '127.0.0.1']):
        seed_addresses = 0
        for i in range(size):
            node_port = start_port + i
            node_id = constants.generate_id(i)
            node = await self.create_node(node_id, node_port, external_ip=external_ip)
            self.nodes.append(node)
            node_addresses = [(ip, node_port) for ip in external_ip]
            seed_addresses += len(node_addresses) if i < seed_nodes else 0
            self.known_node_addresses.extend(node_addresses)

        for node in self.nodes:
            node.start(external_ip, self.known_node_addresses[:seed_addresses])

    async def _test_replace_bad_nodes(self, local_addr):
        await self.setup_network(20)
        await asyncio.gather(*[node.joined.wait() for node in self.nodes])
        self.assertEqual(len(self.nodes), 20)
        node = self.nodes[0]
        bad_peers = []
        for candidate in self.nodes[1:10]:
            proto = candidate.protocols[local_addr]
            address, port, node_id = proto.external_ip, proto.udp_port, proto.node_id
            peer = make_kademlia_peer(node_id, address, udp_port=port)
            bad_peers.append(peer)
            proto.add_peer(peer)
            candidate.stop()
        await asyncio.sleep(.3)  # let pending events settle
        for bad_peer in bad_peers:
            self.assertIn(bad_peer, node.protocols[local_addr].routing_table.get_peers())
        await node.refresh_node(True)
        await asyncio.sleep(.3)  # let pending events settle
        good_nodes = {good_node.protocols[local_addr].node_id for good_node in self.nodes[10:]}
        for peer in node.protocol.routing_table.get_peers():
            self.assertIn(peer.node_id, good_nodes)

    async def test_replace_bad_nodes_v4(self):
        await self._test_replace_bad_nodes('127.0.0.1')

    async def test_replace_bad_nodes_v6(self):
        await self._test_replace_bad_nodes('::1')

    async def _test_re_join(self, local_addr):
        await self.setup_network(20, seed_nodes=10)
        await asyncio.gather(*[node.joined.wait() for node in self.nodes])
        node = self.nodes[-1]
        self.assertTrue(node.joined.is_set())
        self.assertTrue(node.protocols[local_addr].routing_table.get_peers())
        for network_node in self.nodes[:-1]:
            network_node.stop()
        await node.refresh_node(True)
        await asyncio.sleep(.3)  # let pending events settle
        self.assertFalse(node.protocols[local_addr].routing_table.get_peers())
        for network_node in self.nodes[:-1]:
            network_node.start(['::1', '127.0.0.1'], self.known_node_addresses)
        self.assertFalse(node.protocols[local_addr].routing_table.get_peers())
        timeout = 20
        while not node.protocols[local_addr].routing_table.get_peers():
            await asyncio.sleep(.1)
            timeout -= 1
            if not timeout:
                self.fail("node didn't join back after 2 seconds")

    async def test_re_join_v4(self):
        await self._test_re_join('127.0.0.1')

    async def test_re_join_v6(self):
        await self._test_re_join('::1')

    async def test_announce_no_peers(self):
        await self.setup_network(1)
        node = self.nodes[0]
        blob_hash = hexlify(constants.generate_id(1337)).decode()
        peers = await node.announce_blob(blob_hash)
        self.assertEqual(len(peers), 0)

    async def test_get_token_on_announce(self):
        await self.setup_network(2, seed_nodes=2)
        await asyncio.gather(*[node.joined.wait() for node in self.nodes])
        node1, node2 = self.nodes
        for proto1 in node1.protocols.values():
            for proto2 in node2.protocols.values():
                proto1.peer_manager.clear_token(proto2.node_id)
        blob_hash = hexlify(constants.generate_id(1337)).decode()
        node_ids = await node1.announce_blob(blob_hash)
        for proto2 in node2.protocols.values():
            self.assertIn(proto2.node_id, node_ids)
        for proto2 in node2.protocols.values():
            proto2.node_rpc.refresh_token()
        node_ids = await node1.announce_blob(blob_hash)
        for proto2 in node2.protocols.values():
            self.assertIn(proto2.node_id, node_ids)
        for proto2 in node2.protocols.values():
            proto2.node_rpc.refresh_token()
        node_ids = await node1.announce_blob(blob_hash)
        for proto2 in node2.protocols.values():
            self.assertIn(proto2.node_id, node_ids)

    async def _test_peer_search_removes_bad_peers(self, local_addr):
        # that's an edge case discovered by Tom, but an important one
        # imagine that you only got bad peers and refresh will happen in one hour
        # instead of failing for one hour we should be able to recover by scheduling pings to bad peers we find
        await self.setup_network(2, seed_nodes=2)
        await asyncio.gather(*[node.joined.wait() for node in self.nodes])
        node1, node2 = self.nodes
        node2.stop()
        # forcefully make it a bad peer but don't remove it from routing table
        proto2 = node2.protocols[local_addr]
        address, port, node_id = proto2.external_ip, proto2.udp_port, proto2.node_id
        peer = make_kademlia_peer(node_id, address, udp_port=port)
        self.assertTrue(node1.protocols[local_addr].peer_manager.peer_is_good(peer))
        proto1 = node1.protocols[local_addr]
        proto1.peer_manager.report_failure(proto2.external_ip, proto2.udp_port)
        proto1.peer_manager.report_failure(proto2.external_ip, proto2.udp_port)
        self.assertFalse(proto1.peer_manager.peer_is_good(peer))

        # now a search happens, which removes bad peers while contacting them
        self.assertTrue(proto1.routing_table.get_peers())
        await node1.peer_search(proto2.node_id)
        await asyncio.sleep(.3)  # let pending events settle
        self.assertFalse(proto1.routing_table.get_peers())

    async def test_peer_search_removes_bad_peers_v4(self):
        await self._test_peer_search_removes_bad_peers('127.0.0.1')

    async def test_peer_search_removes_bad_peers_v6(self):
        await self._test_peer_search_removes_bad_peers('::1')

    async def test_peer_persistance(self):
        num_nodes = 6
        start_port = 40000
        num_seeds = 2
        #external_ip = ['0.0.0.0', '::']
        external_ip = ['::1', '127.0.0.1']
        #external_ip = ['::1']
        #external_ip = ['127.0.0.1']

        # Start a node
        await self.setup_network(num_nodes, start_port=start_port, seed_nodes=num_seeds, external_ip=external_ip)
        await asyncio.gather(*[node.joined.wait() for node in self.nodes])

        node1 = self.nodes[-1]
        peer_args = [
            (p.node_id, p.external_ip, p.udp_port, p.peer_port)
            for n in self.nodes[:num_seeds]
            for p in n.protocols.values()
        ]
        peers = [make_kademlia_peer(*args) for args in peer_args]

        # node1 is bootstrapped from the fixed seeds
        self.assertCountEqual(peers, [p for proto in node1.protocols.values() for p in proto.routing_table.get_peers()])

        # Refresh and assert that the peers were persisted
        await node1.refresh_node(True)
        self.assertEqual(len(peer_args), len(await node1._storage.get_persisted_kademlia_peers()))
        node1.stop()

        # Start a fresh node with the same node_id and storage, but no known peers
        node2 = await self.create_node(constants.generate_id(num_nodes-1), start_port+num_nodes-1, external_ip)
        node2._storage = node1._storage
        node2.start(external_ip, [])
        await node2.joined.wait()

        await asyncio.sleep(1)

        # The peers are restored
        seed_addrs = [p.external_ip for n in self.nodes[:num_seeds] for p in n.protocols.values()]
        peers = [p for proto in node2.protocols.values() for p in proto.routing_table.get_peers()]
        self.assertEqual(len(seed_addrs), len(peers), peers)
        for bucket1, bucket2 in zip([b for p in node1.protocols.values() for b in p.routing_table.buckets],
                                    [b for p in node2.protocols.values() for b in p.routing_table.buckets]):
            self.assertEqual((bucket1.range_min, bucket1.range_max), (bucket2.range_min, bucket2.range_max))
