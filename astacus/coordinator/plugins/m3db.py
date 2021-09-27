"""Copyright (c) 2020 Aiven Ltd
See LICENSE for details

m3db backup/restore plugin

All of the actual heavy lifting is done using the base file
snapshot/restore functionality. m3db plugin will simply ensure etcd
state is consistent.

"""
from .base import BackupOpBase, RestoreOpBase
from .etcd import ETCDConfiguration, ETCDDump, get_etcd_dump, restore_etcd_dump
from astacus.common import exceptions, ipc, m3placement
from astacus.common.etcd import ETCDClient
from astacus.common.utils import AstacusModel
from astacus.coordinator.cluster import Cluster
from typing import List, Optional

import logging

logger = logging.getLogger(__name__)


class M3IncorrectPlacementNodesLengthException(exceptions.PermanentException):
    pass


class M3DBConfiguration(ETCDConfiguration):
    environment: str
    placement_nodes: List[m3placement.M3PlacementNode]


class M3DBManifest(AstacusModel):
    etcd: ETCDDump
    placement_nodes: List[m3placement.M3PlacementNode]


def _validate_m3_config(o):
    pnode_count = len(o.plugin_config.placement_nodes)
    node_count = len(o.nodes)
    if pnode_count != node_count:
        diff = node_count - pnode_count
        raise M3IncorrectPlacementNodesLengthException(
            f"{node_count} nodes, yet {pnode_count} nodes in the m3 placement_nodes; difference of {diff}"
        )
    return True


class M3DBBackupOp(BackupOpBase):
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

    async def step_init(self, cluster: Cluster):
        env = self.plugin_config.environment
        self.etcd_prefixes = [p.format(env=env).encode() for p in self.etcd_prefix_formats]
        _validate_m3_config(self)
        return True

    async def step_retrieve_etcd(self, cluster: Cluster):
        return await get_etcd_dump(ETCDClient(self.plugin_config.etcd_url), self.etcd_prefixes)

    async def step_retrieve_etcd_again(self, cluster: Cluster):
        etcd_now = await get_etcd_dump(ETCDClient(self.plugin_config.etcd_url), self.etcd_prefixes)
        return etcd_now == self.result_retrieve_etcd

    async def step_create_m3_manifest(self, cluster: Cluster):
        m3manifest = M3DBManifest(etcd=self.result_retrieve_etcd, placement_nodes=self.plugin_config.placement_nodes)
        self.plugin_data = m3manifest.dict()
        return m3manifest


class M3DRestoreOp(RestoreOpBase):
    plugin = ipc.Plugin.m3db
    steps = [
        "init",  # local
        "backup_name",  # base -->
        "backup_manifest",
        "rewrite_etcd",  # local -->
        "restore_etcd",
        "restore",  # base
    ]

    result_rewrite_etcd: Optional[ETCDDump] = None

    async def step_init(self, cluster: Cluster):
        _validate_m3_config(self)
        return True

    def _rewrite_m3db_placement(self, key):
        replacements = []
        node_to_backup_index = self._get_node_to_backup_index()
        for idx, node, pnode in zip(node_to_backup_index, self.nodes, self.plugin_config.placement_nodes):
            if idx is None:
                continue
            src_pnode = self.plugin_manifest.placement_nodes[idx]
            replacements.append(
                m3placement.M3PlacementNodeReplacement(src_pnode=src_pnode, dst_isolation_group=node.az, dst_pnode=pnode)
            )
        value = key.value_bytes
        value = m3placement.rewrite_m3_placement_bytes(value, replacements)
        key.set_value_bytes(value)

    async def step_rewrite_etcd(self, cluster: Cluster):
        if self.req.partial_restore_nodes:
            logger.debug("Skipping etcd rewrite due to partial backup restoration")
            return True
        etcd = self.plugin_manifest.etcd.copy(deep=True)
        for prefix in etcd.prefixes:
            for key in prefix.keys:
                key_bytes = key.key_bytes
                if key_bytes.startswith(b"_sd.placement/") and key_bytes.endswith(b"/m3db"):
                    self._rewrite_m3db_placement(key)
        return etcd

    async def step_restore_etcd(self, cluster: Cluster):
        if self.req.partial_restore_nodes:
            logger.debug("Skipping etcd restoration due to partial backup restoration")
            return True
        assert self.result_rewrite_etcd is not None
        return await restore_etcd_dump(ETCDClient(self.plugin_config.etcd_url), self.result_rewrite_etcd)


plugin_info = {"backup": M3DBBackupOp, "manifest": M3DBManifest, "restore": M3DRestoreOp, "config": M3DBConfiguration}
