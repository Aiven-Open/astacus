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
from typing import Dict, List, Optional

import asyncio


class ETCDConfiguration(AstacusModel):
    # etcd v3 API has to be reachable using this URL by coordinator
    etcd_url: str


class ETCDPrefixDump(AstacusModel):
    prefix_b64: str
    etcd_key_values_b64: Dict[str, str]


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
        return ETCDPrefixDump(prefix_b64=b64encode(prefix), etcd_key_values_b64=kvs)

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
        for k, v in prefix.etcd_key_values_b64.items():
            if not await self._etcd_client.kv_put_b64(key=k, value=v):
                return False
        return True

    async def restore_etcd_dump(self, dump: ETCDDump) -> bool:
        for prefix in dump.prefixes:
            if not await self._restore_etcd_prefix(prefix):
                return False
        return True
