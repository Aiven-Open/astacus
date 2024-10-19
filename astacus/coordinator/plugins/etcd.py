"""Copyright (c) 2020 Aiven Ltd
See LICENSE for details.

ETCD backup/restore base; while in theory someone might want to backup
etcd in isolation, this isn't really the tool for it.

"""

from astacus.common.etcd import ETCDClient
from astacus.common.utils import AstacusModel
from base64 import b64decode, b64encode
from collections.abc import Sequence

import asyncio
import base64


class ETCDConfiguration(AstacusModel):
    # etcd v3 API has to be reachable using this URL by coordinator
    etcd_url: str


class ETCDKey(AstacusModel):
    key_b64: str
    value_b64: str

    @property
    def key_bytes(self):
        return base64.b64decode(self.key_b64)

    def set_key_bytes(self, value):
        self.key_b64 = base64.b64encode(value).decode()

    @property
    def value_bytes(self):
        return base64.b64decode(self.value_b64)

    def set_value_bytes(self, value):
        self.value_b64 = base64.b64encode(value).decode()


class ETCDPrefixDump(AstacusModel):
    prefix_b64: str
    keys: Sequence[ETCDKey]


class ETCDDump(AstacusModel):
    prefixes: Sequence[ETCDPrefixDump]


async def get_etcd_dump(client: ETCDClient, prefixes: Sequence[bytes]) -> ETCDDump | None:
    prefix_dump_coros = [_get_etcd_prefix_dump(client, prefix) for prefix in prefixes]
    prefix_ranges = await asyncio.gather(*prefix_dump_coros)
    if any(True for prefix_range in prefix_ranges if prefix_range is None):
        return None
    non_none_prefix_ranges = [prefix_range for prefix_range in prefix_ranges if prefix_range is not None]
    return ETCDDump(prefixes=non_none_prefix_ranges)


async def restore_etcd_dump(client: ETCDClient, dump: ETCDDump) -> bool:
    for prefix in dump.prefixes:
        if not await _restore_etcd_prefix(client, prefix):
            return False
    return True


async def _get_etcd_prefix_dump(client: ETCDClient, prefix: bytes) -> ETCDPrefixDump | None:
    assert isinstance(prefix, bytes)
    kvs = await client.prefix_get(prefix)
    if not kvs:
        return None
    keys = [ETCDKey(key_b64=k, value_b64=v) for k, v in kvs.items()]
    return ETCDPrefixDump(prefix_b64=b64encode(prefix).decode(), keys=keys)


async def _restore_etcd_prefix(client: ETCDClient, prefix: ETCDPrefixDump) -> bool:
    # Delete the whole prefix from etcd first
    if not await client.prefix_delete(b64decode(prefix.prefix_b64)):
        return False
    # Then put the entries we had for the prefix
    for key in prefix.keys:
        if not await client.kv_put_b64(key=key.key_b64, value=key.value_b64):
            return False
    return True
