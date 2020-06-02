"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

ETCD backup/restore base; while in theory someone might want to backup
etcd in isolation, this isn't really the tool for it.

"""

from .base import BackupOpBase, RestoreOpBase
from astacus.common.etcd import ETCDClient
from astacus.common.utils import AstacusModel
from base64 import b64decode, b64encode
from typing import List, Optional

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
    keys: List[ETCDKey]


class ETCDDump(AstacusModel):
    prefixes: List[ETCDPrefixDump]


class ETCDBackupOpBase(BackupOpBase):
    @property
    def _etcd_client(self):
        url = self.plugin_config.etcd_url
        return ETCDClient(url)

    async def _get_etcd_prefix_dump(self, prefix: bytes) -> Optional[ETCDPrefixDump]:
        client = self._etcd_client
        assert isinstance(prefix, bytes)
        kvs = await client.prefix_get(prefix)
        if not kvs:
            return None
        keys = [ETCDKey(key_b64=k, value_b64=v) for k, v in kvs.items()]
        return ETCDPrefixDump(prefix_b64=b64encode(prefix), keys=keys)

    async def get_etcd_dump(self, prefixes) -> Optional[ETCDDump]:
        prefixes = [self._get_etcd_prefix_dump(prefix) for prefix in prefixes]
        prefixes = await asyncio.gather(*prefixes)
        if any(True for range in prefixes if range is None):
            return None
        return ETCDDump(prefixes=prefixes)


class ETCDRestoreOpBase(RestoreOpBase):
    @property
    def _etcd_client(self):
        url = self.plugin_config.etcd_url
        return ETCDClient(url)

    async def _restore_etcd_prefix(self, prefix) -> bool:
        # Delete the whole prefix from etcd first
        if not await self._etcd_client.prefix_delete(b64decode(prefix.prefix_b64)):
            return False
        # Then put the entries we had for the prefix
        for key in prefix.keys:
            if not await self._etcd_client.kv_put_b64(key=key.key_b64, value=key.value_b64):
                return False
        return True

    async def restore_etcd_dump(self, dump: ETCDDump) -> bool:
        for prefix in dump.prefixes:
            if not await self._restore_etcd_prefix(prefix):
                return False
        return True
