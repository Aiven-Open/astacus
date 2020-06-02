"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

M3 backup plugin

All of the actual heavy lifting is done using the base file
snapshot/restore functionality. M3 plugin will simply ensure etcd
state is consistent.
"""

from .etcd import ETCDBackupOpBase, ETCDConfiguration, ETCDDump, ETCDRestoreOpBase
from astacus.common import ipc
from astacus.common.utils import AstacusModel
from typing import List, Optional


class M3DBConfiguration(ETCDConfiguration):
    environment: str


class M3DBManifest(AstacusModel):
    etcd: ETCDDump


class M3DBBackupOp(ETCDBackupOpBase):
    # upload backup manifest only after we've retrieved again etcd
    # contents and found it consistent
    steps = [
        "init",  # local -->
        "retrieve_etcd",
        "snapshot",  # base -->
        "list_hexdigests",
        "upload_blocks",
        "retrieve_etcd_again",  # local -->
        "create_m3_manifest",
        "upload_manifest",  # base
    ]

    plugin = ipc.Plugin.m3db

    result_retrieve_etcd: Optional[ETCDDump] = None

    snapshot_root_globs = ["**/*.db"]

    etcd_prefix_formats = ["_kv/{env}/m3db.node.namespaces", "_sd.placement/{env}/m3db"]

    etcd_prefixes: List[bytes] = []

    async def step_init(self):
        env = self.plugin_config.environment
        self.etcd_prefixes = [p.format(env=env).encode() for p in self.etcd_prefix_formats]
        return True

    async def step_retrieve_etcd(self):
        return await self.get_etcd_dump(self.etcd_prefixes)

    async def step_retrieve_etcd_again(self):
        etcd_now = await self.get_etcd_dump(self.etcd_prefixes)
        return etcd_now == self.result_retrieve_etcd

    async def step_create_m3_manifest(self):
        m3manifest = M3DBManifest(etcd=self.result_retrieve_etcd)
        self.plugin_data = m3manifest.dict()
        return m3manifest


class M3DRestoreOp(ETCDRestoreOpBase):
    plugin = ipc.Plugin.m3db
    steps = [
        "backup_name",  # base -->
        "backup_manifest",
        "restore_etcd",  # local
        "restore",  # base
    ]
    plugin = ipc.Plugin.m3db

    async def step_restore_etcd(self):
        return await self.restore_etcd_dump(self.plugin_manifest.etcd)


plugin_info = {"backup": M3DBBackupOp, "manifest": M3DBManifest, "restore": M3DRestoreOp, "config": M3DBConfiguration}
