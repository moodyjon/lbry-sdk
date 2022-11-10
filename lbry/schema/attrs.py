import json
import logging
import os.path
import hashlib
from collections.abc import MutableMapping, MutableSet, Iterable
from typing import Tuple, List
from string import ascii_letters
from decimal import Decimal, ROUND_UP
from binascii import hexlify, unhexlify
from google.protobuf.json_format import MessageToDict

from lbry.crypto.base58 import Base58
from lbry.constants import COIN
from lbry.error import MissingPublishedFileError, EmptyPublishedFileError

import lbry.schema.claim as claim
from lbry.schema.mime_types import guess_media_type
from lbry.schema.base import Metadata, BaseMessageList
from lbry.schema.tags import clean_tags, normalize_tag
from google.protobuf.message import Message as ProtobufMessage
from lbry.schema.types.v2.claim_pb2 import (
    Claim as ClaimMessage,
    Fee as FeeMessage,
    Location as LocationMessage,
    Language as LanguageMessage,
    Stream as StreamMessage,
)
from lbry.schema.types.v2.extension_pb2 import (
    StringMap as StringMapMessage,
    Extension as ExtensionMessage,
)

log = logging.getLogger(__name__)


def calculate_sha384_file_hash(file_path):
    sha384 = hashlib.sha384()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(128 * sha384.block_size), b''):
            sha384.update(chunk)
    return sha384.digest()


def country_int_to_str(country: int) -> str:
    r = LocationMessage.Country.Name(country)
    return r[1:] if r.startswith('R') else r


def country_str_to_int(country: str) -> int:
    if len(country) == 3:
        country = 'R' + country
    return LocationMessage.Country.Value(country)


class Dimmensional(Metadata):

    __slots__ = ()

    @property
    def width(self) -> int:
        return self.message.width

    @width.setter
    def width(self, width: int):
        self.message.width = width

    @property
    def height(self) -> int:
        return self.message.height

    @height.setter
    def height(self, height: int):
        self.message.height = height

    @property
    def dimensions(self) -> Tuple[int, int]:
        return self.width, self.height

    @dimensions.setter
    def dimensions(self, dimensions: Tuple[int, int]):
        self.message.width, self.message.height = dimensions

    def _extract(self, file_metadata, field):
        try:
            setattr(self, field, file_metadata.getValues(field)[0])
        except:
            log.exception(f'Could not extract {field} from file metadata.')

    def update(self, file_metadata=None, height=None, width=None):
        if height is not None:
            self.height = height
        elif file_metadata:
            self._extract(file_metadata, 'height')

        if width is not None:
            self.width = width
        elif file_metadata:
            self._extract(file_metadata, 'width')


class Playable(Metadata):

    __slots__ = ()

    @property
    def duration(self) -> int:
        return self.message.duration

    @duration.setter
    def duration(self, duration: int):
        self.message.duration = duration

    def update(self, file_metadata=None, duration=None):
        if duration is not None:
            self.duration = duration
        elif file_metadata:
            try:
                self.duration = file_metadata.getValues('duration')[0].seconds
            except:
                log.exception('Could not extract duration from file metadata.')


class Image(Dimmensional):

    __slots__ = ()


class Audio(Playable):

    __slots__ = ()


class Video(Dimmensional, Playable):

    __slots__ = ()

    def update(self, file_metadata=None, height=None, width=None, duration=None):
        Dimmensional.update(self, file_metadata, height, width)
        Playable.update(self, file_metadata, duration)


class Source(Metadata):

    __slots__ = ()

    def update(self, file_path=None):
        if file_path is not None:
            self.name = os.path.basename(file_path)
            self.media_type, stream_type = guess_media_type(file_path)
            if not os.path.isfile(file_path):
                raise MissingPublishedFileError(file_path)
            self.size = os.path.getsize(file_path)
            if self.size == 0:
                raise EmptyPublishedFileError(file_path)
            self.file_hash_bytes = calculate_sha384_file_hash(file_path)
            return stream_type

    @property
    def name(self) -> str:
        return self.message.name

    @name.setter
    def name(self, name: str):
        self.message.name = name

    @property
    def size(self) -> int:
        return self.message.size

    @size.setter
    def size(self, size: int):
        self.message.size = size

    @property
    def media_type(self) -> str:
        return self.message.media_type

    @media_type.setter
    def media_type(self, media_type: str):
        self.message.media_type = media_type

    @property
    def file_hash(self) -> str:
        return hexlify(self.message.hash).decode()

    @file_hash.setter
    def file_hash(self, file_hash: str):
        self.message.hash = unhexlify(file_hash.encode())

    @property
    def file_hash_bytes(self) -> bytes:
        return self.message.hash

    @file_hash_bytes.setter
    def file_hash_bytes(self, file_hash_bytes: bytes):
        self.message.hash = file_hash_bytes

    @property
    def sd_hash(self) -> str:
        return hexlify(self.message.sd_hash).decode()

    @sd_hash.setter
    def sd_hash(self, sd_hash: str):
        self.message.sd_hash = unhexlify(sd_hash.encode())

    @property
    def sd_hash_bytes(self) -> bytes:
        return self.message.sd_hash

    @sd_hash_bytes.setter
    def sd_hash_bytes(self, sd_hash: bytes):
        self.message.sd_hash = sd_hash

    @property
    def bt_infohash(self) -> str:
        return hexlify(self.message.bt_infohash).decode()

    @bt_infohash.setter
    def bt_infohash(self, bt_infohash: str):
        self.message.bt_infohash = unhexlify(bt_infohash.encode())

    @property
    def bt_infohash_bytes(self) -> bytes:
        return self.message.bt_infohash.decode()

    @bt_infohash_bytes.setter
    def bt_infohash_bytes(self, bt_infohash: bytes):
        self.message.bt_infohash = bt_infohash

    @property
    def url(self) -> str:
        return self.message.url

    @url.setter
    def url(self, url: str):
        self.message.url = url


class Fee(Metadata):

    __slots__ = ()

    def update(self, address: str = None, currency: str = None, amount=None):
        if amount:
            currency = (currency or self.currency or '').lower()
            if not currency:
                raise Exception('In order to set a fee amount, please specify a fee currency.')
            if currency not in ('lbc', 'btc', 'usd'):
                raise Exception(f'Missing or unknown currency provided: {currency}')
            setattr(self, currency, Decimal(amount))
        elif currency:
            raise Exception('In order to set a fee currency, please specify a fee amount.')
        if address:
            if not self.currency:
                raise Exception('In order to set a fee address, please specify a fee amount and currency.')
            self.address = address

    @property
    def currency(self) -> str:
        if self.message.currency:
            return FeeMessage.Currency.Name(self.message.currency)

    @property
    def address(self) -> str:
        if self.address_bytes:
            return Base58.encode(self.address_bytes)

    @address.setter
    def address(self, address: str):
        self.address_bytes = Base58.decode(address)

    @property
    def address_bytes(self) -> bytes:
        return self.message.address

    @address_bytes.setter
    def address_bytes(self, address: bytes):
        self.message.address = address

    @property
    def amount(self) -> Decimal:
        if self.currency == 'LBC':
            return self.lbc
        if self.currency == 'BTC':
            return self.btc
        if self.currency == 'USD':
            return self.usd

    DEWIES = Decimal(COIN)

    @property
    def lbc(self) -> Decimal:
        if self.message.currency != FeeMessage.LBC:
            raise ValueError('LBC can only be returned for LBC fees.')
        return Decimal(self.message.amount / self.DEWIES)

    @lbc.setter
    def lbc(self, amount: Decimal):
        self.dewies = int(amount * self.DEWIES)

    @property
    def dewies(self) -> int:
        if self.message.currency != FeeMessage.LBC:
            raise ValueError('Dewies can only be returned for LBC fees.')
        return self.message.amount

    @dewies.setter
    def dewies(self, amount: int):
        self.message.amount = amount
        self.message.currency = FeeMessage.LBC

    SATOSHIES = Decimal(COIN)

    @property
    def btc(self) -> Decimal:
        if self.message.currency != FeeMessage.BTC:
            raise ValueError('BTC can only be returned for BTC fees.')
        return Decimal(self.message.amount / self.SATOSHIES)

    @btc.setter
    def btc(self, amount: Decimal):
        self.satoshis = int(amount * self.SATOSHIES)

    @property
    def satoshis(self) -> int:
        if self.message.currency != FeeMessage.BTC:
            raise ValueError('Satoshies can only be returned for BTC fees.')
        return self.message.amount

    @satoshis.setter
    def satoshis(self, amount: int):
        self.message.amount = amount
        self.message.currency = FeeMessage.BTC

    PENNIES = Decimal('100.0')
    PENNY = Decimal('0.01')

    @property
    def usd(self) -> Decimal:
        if self.message.currency != FeeMessage.USD:
            raise ValueError('USD can only be returned for USD fees.')
        return Decimal(self.message.amount / self.PENNIES)

    @usd.setter
    def usd(self, amount: Decimal):
        self.pennies = int(amount.quantize(self.PENNY, ROUND_UP) * self.PENNIES)

    @property
    def pennies(self) -> int:
        if self.message.currency != FeeMessage.USD:
            raise ValueError('Pennies can only be returned for USD fees.')
        return self.message.amount

    @pennies.setter
    def pennies(self, amount: int):
        self.message.amount = amount
        self.message.currency = FeeMessage.USD


class ClaimReference(Metadata):

    __slots__ = ()

    @property
    def claim_id(self) -> str:
        return hexlify(self.claim_hash[::-1]).decode()

    @claim_id.setter
    def claim_id(self, claim_id: str):
        self.claim_hash = unhexlify(claim_id)[::-1]

    @property
    def claim_hash(self) -> bytes:
        return self.message.claim_hash

    @claim_hash.setter
    def claim_hash(self, claim_hash: bytes):
        self.message.claim_hash = claim_hash

class ModifyingClaimReference(ClaimReference):

    __slots__ = ()

    @property
    def modification_type(self) -> str:
        return self.message.WhichOneof('type')

    @modification_type.setter
    def modification_type(self, claim_type: str):
        """Select the appropriate member (stream, channel, repost, or collection)"""
        old_type = self.message.WhichOneof('type')
        if old_type == claim_type:
            return
        if old_type and claim_type is None:
            self.message.ClearField(old_type)
            return
        member = getattr(self.message, claim_type)
        member.SetInParent()

    def update(self, claim_type: str, **kwargs) -> dict:
        """
        Store updates to modifiable fields in deletions/edits.
        Currently, only the "extensions" field (StreamExtensionList)
        of a stream claim may be modified. Returns a dict containing
        the unhandled portion of "kwargs".
        """
        if claim_type != 'stream':
            return kwargs

        clr_exts = kwargs.pop('clear_extensions', None)
        set_exts = kwargs.pop('extensions', None)
        if clr_exts is None and set_exts is None:
            return kwargs

        self.modification_type = claim_type
        if not self.modification_type == 'stream':
            return kwargs

        mods = getattr(self.message, self.modification_type)

        if clr_exts is not None:
            deletions = StreamModifiable(mods.deletions)
            if isinstance(clr_exts, str) and clr_exts.startswith('{'):
                clr_exts = json.loads(clr_exts)
            deletions.extensions.merge(clr_exts)

        if set_exts is not None:
            edits = StreamModifiable(mods.edits)
            if isinstance(set_exts, str) and set_exts.startswith('{'):
                set_exts = json.loads(set_exts)
            edits.extensions.merge(set_exts)

        return kwargs

    def apply(self, reposted: 'claim.Claim') -> 'claim.Claim':
        """
        Given a reposted claim, apply the stored deletions/edits, and return
        the modified claim. Returns the original claim if the claim type has
        changed such that the modifications are not relevant.
        """
        if not self.modification_type or self.modification_type != reposted.claim_type:
            return reposted
        if not reposted.claim_type == 'stream':
            return reposted

        m = ClaimMessage()
        m.CopyFrom(reposted.message)
        result = claim.Claim(m)

        # only stream claims, and only stream extensions are handled
        stream = getattr(result, result.claim_type)
        exts = getattr(stream, 'extensions')

        mods = getattr(self.message, self.modification_type)
        # apply deletions
        exts.merge(StreamModifiable(mods.deletions).extensions, delete=True)
        # apply edits
        exts.merge(StreamModifiable(mods.edits).extensions)
        return result

class ClaimList(BaseMessageList[ClaimReference]):

    __slots__ = ()
    item_class = ClaimReference

    @property
    def _message(self):
        return self.message.claim_references

    def append(self, value):
        self.add().claim_id = value

    @property
    def ids(self) -> List[str]:
        return [c.claim_id for c in self]


class Language(Metadata):

    __slots__ = ()

    @property
    def langtag(self) -> str:
        langtag = []
        if self.language:
            langtag.append(self.language)
        if self.script:
            langtag.append(self.script)
        if self.region:
            langtag.append(self.region)
        return '-'.join(langtag)

    @langtag.setter
    def langtag(self, langtag: str):
        parts = langtag.split('-')
        self.language = parts.pop(0)
        if parts and len(parts[0]) == 4:
            self.script = parts.pop(0)
        if parts and len(parts[0]) == 2 and parts[0].isalpha():
            self.region = parts.pop(0)
        if parts and len(parts[0]) == 3 and parts[0].isdigit():
            self.region = parts.pop(0)
        assert not parts, f"Failed to parse language tag: {langtag}"

    @property
    def language(self) -> str:
        if self.message.language:
            return LanguageMessage.Language.Name(self.message.language)

    @language.setter
    def language(self, language: str):
        self.message.language = LanguageMessage.Language.Value(language)

    @property
    def script(self) -> str:
        if self.message.script:
            return LanguageMessage.Script.Name(self.message.script)

    @script.setter
    def script(self, script: str):
        self.message.script = LanguageMessage.Script.Value(script)

    @property
    def region(self) -> str:
        if self.message.region:
            return country_int_to_str(self.message.region)

    @region.setter
    def region(self, region: str):
        self.message.region = country_str_to_int(region)


class LanguageList(BaseMessageList[Language]):
    __slots__ = ()
    item_class = Language

    def append(self, value: str):
        self.add().langtag = value


class Location(Metadata):

    __slots__ = ()

    def from_value(self, value):
        if isinstance(value, str) and value.startswith('{'):
            value = json.loads(value)

        if isinstance(value, dict):
            for key, val in value.items():
                setattr(self, key, val)

        elif isinstance(value, str):
            parts = value.split(':')
            if len(parts) > 2 or (parts[0] and parts[0][0] in ascii_letters):
                country = parts and parts.pop(0)
                if country:
                    self.country = country
                state = parts and parts.pop(0)
                if state:
                    self.state = state
                city = parts and parts.pop(0)
                if city:
                    self.city = city
                code = parts and parts.pop(0)
                if code:
                    self.code = code
            latitude = parts and parts.pop(0)
            if latitude:
                self.latitude = latitude
            longitude = parts and parts.pop(0)
            if longitude:
                self.longitude = longitude

        else:
            raise ValueError(f'Could not parse country value: {value}')

    def to_dict(self):
        d = MessageToDict(self.message)
        if self.message.longitude:
            d['longitude'] = self.longitude
        if self.message.latitude:
            d['latitude'] = self.latitude
        return d

    @property
    def country(self) -> str:
        if self.message.country:
            return LocationMessage.Country.Name(self.message.country)

    @country.setter
    def country(self, country: str):
        self.message.country = LocationMessage.Country.Value(country)

    @property
    def state(self) -> str:
        return self.message.state

    @state.setter
    def state(self, state: str):
        self.message.state = state

    @property
    def city(self) -> str:
        return self.message.city

    @city.setter
    def city(self, city: str):
        self.message.city = city

    @property
    def code(self) -> str:
        return self.message.code

    @code.setter
    def code(self, code: str):
        self.message.code = code

    GPS_PRECISION = Decimal('10000000')

    @property
    def latitude(self) -> str:
        if self.message.latitude:
            return str(Decimal(self.message.latitude) / self.GPS_PRECISION)

    @latitude.setter
    def latitude(self, latitude: str):
        latitude = Decimal(latitude)
        assert -90 <= latitude <= 90, "Latitude must be between -90 and 90 degrees."
        self.message.latitude = int(latitude * self.GPS_PRECISION)

    @property
    def longitude(self) -> str:
        if self.message.longitude:
            return str(Decimal(self.message.longitude) / self.GPS_PRECISION)

    @longitude.setter
    def longitude(self, longitude: str):
        longitude = Decimal(longitude)
        assert -180 <= longitude <= 180, "Longitude must be between -180 and 180 degrees."
        self.message.longitude = int(longitude * self.GPS_PRECISION)


class LocationList(BaseMessageList[Location]):
    __slots__ = ()
    item_class = Location

    def append(self, value):
        self.add().from_value(value)


class TagList(BaseMessageList[str]):
    __slots__ = ()
    item_class = str

    def append(self, tag: str):
        tag = normalize_tag(tag)
        if tag and tag not in self.message:
            self.message.append(tag)

class StreamExtension(Metadata):
    __slots__ = Metadata.__slots__ + ('extension_schema',)

    def __init__(self, schema, message):
        if isinstance(message, StringMapMessage):
            wrapper = ExtensionMessage()
            wrapper.map.CopyFrom(message)
            super().__init__(wrapper)
        else:
            assert isinstance(message, ProtobufMessage) and message.DESCRIPTOR.full_name == 'pb.Extension'
            super().__init__(message)
        self.extension_schema = schema

    def to_dict(self, include_schema=True):
        attrs = self.unpacked.to_dict()
        return { f'{self.schema}': attrs } if include_schema else attrs

    def from_value(self, value):
        schema = self.schema

        # If incoming is an extension, we have an Extension message.
        if isinstance(value, StreamExtension):
            schema = value.schema or schema

        # Translate str -> (JSON) dict.
        if isinstance(value, str) and value.startswith('{'):
            value = json.loads(value)

        # Check for 1-element dictionary at top level: {<schema>: <attrs>}.
        if isinstance(value, dict) and len(value) == 1:
            k = next(iter(value.keys()))
            if self.schema is None or self.schema == k:
                # Schema is determined. Extract dict containining attrs.
                schema = k
                value = value[schema]

        # Try to decode attrs dict -> Extension message containing StringMap.
        if isinstance(value, dict):
            try:
                ext = StreamExtension(schema, ExtensionMessage())
                map = StringMap(ext.message.map)
                for k, v in value.items():
                    vs = StringMapValues(StringMapMessage.Values())
                    if not isinstance(v, list):
                        v = [v]
                    for e in v:
                       vs.add(e)
                    map.update(**{k: vs})
                value = ext
            except:
                log.info('Could not parse StreamExtension value: %s', value, exc_info=True)

        # Either we have an Extension message or decoding failed.
        if isinstance(value, StreamExtension):
            self.extension_schema = value.schema or schema
            self.message.CopyFrom(value.message)
        else:
            log.info('Could not parse StreamExtension value: %s class: %s', value, value.__class__.__name__)
            raise ValueError(f'Could not parse StreamExtension value: {value}')

    @property
    def schema(self):
        return self.extension_schema

    @property
    def unpacked(self):
        return StringMap(self.message.map)

    def merge(self, ext: 'StreamExtension', delete: bool = False) -> 'StreamExtension':
        self.unpacked.merge(ext.unpacked, delete=delete)
        return self

class StringMapValue(Metadata):
    __slots__ = ()

    @property
    def unpacked(self):
        return getattr(self.message, self.message.WhichOneof('type'))

    @unpacked.setter
    def unpacked(self, x):
        if isinstance(x, str):
            setattr(self.message, 'str', x)
        elif isinstance(x, int):
            setattr(self.message, 'int', x)
        else:
            raise TypeError(f'Cannot encode {type(x)} as a StreamExtension property.')
class StringMapValues(Metadata, MutableSet, Iterable):
    __slots__ = ()
    item_class = StringMapValue

    def __contains__(self, x):
        return x in map(lambda x: StringMapValue(x).unpacked, self.message.vs)

    def __iter__(self):
        return iter(map(lambda x: StringMapValue(x).unpacked, self.message.vs))

    def __len__(self):
        return len(self.message.vs)

    def add(self, x):
        if x not in self:
            v = StringMapValue(self.message.vs.add())
            v.unpacked = x

    def discard(self, x):
        for i, v in enumerate(map(lambda x: StringMapValue(x).unpacked, self.message.vs)):
            if v == x:
                del self.message.vs[i]

class StringMap(Metadata, MutableMapping, Iterable):
    __slots__ = ()
    item_class = StringMapValues

    def to_dict(self) -> dict:
        """Generate short form dictionary {"<k1>": "<v1>", "<k2>": ["<v21>", "<v22>"], ...}}"""
        attrs = {}
        for k, v in self.items():
            values = list(v)
            if len(values) > 1:
                attrs.update(**{k: values})
            elif len(values) > 0:
                attrs.update(**{k: values[0]})
        return attrs

    def merge(self, other: 'StringMap', delete: bool = False) -> 'StringMap':
        for k, v in other.items():
            if delete:
                if k not in self:
                    continue
                elif len(v) > 0:
                    self[k] -= v
                else:
                    del self[k]
            else:
                if k not in self:
                    self[k] = v
                else:
                    self[k] |= v
        return self

    def __getitem__(self, key):
        if key in self.message.fields:
            return StringMapValues(self.message.fields[key])
        raise KeyError(key)

    def __setitem__(self, key, value):
        self.message.fields[key].CopyFrom(value.message)

    def __delitem__(self, key):
        del self.message.fields[key]

    def __iter__(self):
        return iter(self.message.fields)

    def __len__(self):
        return len(self.message.fields)

class StreamExtensionMap(Metadata, MutableMapping, Iterable):
    __slots__ = ()
    item_class = StreamExtension

    def to_dict(self):
        return { k: v.to_dict(include_schema=False) for k, v in self.items() }

    def merge(self, exts, delete: bool = False) -> 'StreamExtensionMap':
        if isinstance(exts, StreamExtension):
            exts = {exts.schema: exts}
        if isinstance(exts, str) and exts.startswith('{'):
            exts = json.loads(exts)
        for schema, ext in exts.items():
            obj = StreamExtension(schema, ExtensionMessage())
            if isinstance(ext, StreamExtension):
                obj.from_value(ext)
            else:
                obj.from_value({schema: ext})
            if delete and not len(obj.unpacked):
                del self[schema]
                continue
            existing = StreamExtension(schema, self.message[schema])
            existing.merge(obj, delete=delete)
        return self

    def __getitem__(self, key):
        if key in self.message:
            return StreamExtension(key, self.message[key])
        raise KeyError(key)

    def __setitem__(self, key, value):
        del self.message[key]
        self.message[key].CopyFrom(value)

    def __delitem__(self, key):
        del self.message[key]

    def __iter__(self):
        return self.message.__iter__()

    def __len__(self):
        return len(self.message)


class StreamModifiable(Metadata):
    __slots__ = ()

    @property
    def extensions(self) -> StreamExtensionMap:
        return StreamExtensionMap(self.message.extensions)
