"""Copyright (c) 2020 Aiven Ltd
See LICENSE for details

m3db backup/restore plugin

All of the actual heavy lifting is done using the base file
snapshot/restore functionality. m3db plugin will simply ensure etcd
state is consistent.

"""

from .etcd import ETCDBackupOpBase, ETCDConfiguration, ETCDDump, ETCDRestoreOpBase
from astacus.common import exceptions, ipc
from astacus.common.utils import AstacusModel
from astacus.proto import m3_placement_pb2
from pydantic import validator
from typing import List, Optional

import logging

logger = logging.getLogger(__name__)


class M3IncorrectPlacementNodesLengthException(exceptions.PermanentException):
    pass


MAXIMUM_PROTOBUF_STR_LENGTH = 127


def non_empty_and_sane_length(value):
    if len(value) == 0:
        raise ValueError("Empty value is not supported")
    if len(value) > MAXIMUM_PROTOBUF_STR_LENGTH:
        raise ValueError("Large protobuf fields are not supported")
    return value


class M3PlacementNode(AstacusModel):
    # In Aiven-internal case, most of these are redundant fields (we
    # could derive node_id and hostname from endpoint); however, for
    # generic case, we configure all of them (and expect them to be
    # configured).

    node_id: str
    _validate_node_id = validator("node_id", allow_reuse=True)(non_empty_and_sane_length)

    endpoint: str
    _validate_endpoint = validator("endpoint", allow_reuse=True)(non_empty_and_sane_length)

    hostname: str
    _validate_hostname = validator("hostname", allow_reuse=True)(non_empty_and_sane_length)

    # isolation_group: str # redundant - it is available from generic node snapshot result as az
    # zone/weight: assumed to stay same


class M3DBConfiguration(ETCDConfiguration):
    environment: str
    placement_nodes: List[M3PlacementNode]


class M3DBManifest(AstacusModel):
    etcd: ETCDDump
    placement_nodes: List[M3PlacementNode]


def _validate_m3_config(o):
    pnode_count = len(o.plugin_config.placement_nodes)
    node_count = len(o.nodes)
    if pnode_count != node_count:
        diff = node_count - pnode_count
        raise M3IncorrectPlacementNodesLengthException(
            f"{node_count} nodes, yet {pnode_count} nodes in the m3 placement_nodes; difference of {diff}"
        )
    return True


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
        _validate_m3_config(self)
        return True

    async def step_retrieve_etcd(self):
        return await self.get_etcd_dump(self.etcd_prefixes)

    async def step_retrieve_etcd_again(self):
        etcd_now = await self.get_etcd_dump(self.etcd_prefixes)
        return etcd_now == self.result_retrieve_etcd

    async def step_create_m3_manifest(self):
        m3manifest = M3DBManifest(etcd=self.result_retrieve_etcd, placement_nodes=self.plugin_config.placement_nodes)
        self.plugin_data = m3manifest.dict()
        return m3manifest


def rewrite_single_m3_placement(placement, *, src_pnode: M3PlacementNode, dst_pnode: M3PlacementNode, dst_isolation_group):
    """rewrite single m3 placement entry in-place in protobuf

Relevant places ( see m3db
src/cluster/generated/proto/placementpb/placement.proto which is
copied to astacus/proto/m3_placement.proto )) :

instance<str,Instance> map = 1, and then

message Instance {
  string id                 = 1;
  string isolation_group    = 2;
  string endpoint           = 5;
  string hostname           = 8;
}

    """
    instance = placement.instances[src_pnode.node_id]
    # az may or may not be set; if not, keep original
    if dst_isolation_group:
        instance.isolation_group = dst_isolation_group
    # Copy the rest of the fields
    instance.endpoint = dst_pnode.endpoint
    instance.hostname = dst_pnode.hostname

    # Handle (potential) id change (bit painful as it is also map key)
    instance.id = dst_pnode.node_id
    del placement.instances[src_pnode.node_id]
    placement.instances[dst_pnode.node_id].CopyFrom(instance)


class M3DRestoreOp(ETCDRestoreOpBase):
    plugin = ipc.Plugin.m3db
    steps = [
        "init",  # local
        "backup_name",  # base -->
        "backup_manifest",
        "rewrite_etcd",  # local -->
        "restore_etcd",
        "restore",  # base
    ]
    plugin = ipc.Plugin.m3db

    async def step_init(self):
        _validate_m3_config(self)
        return True

    def _rewrite_m3db_placement(self, key):
        node_to_backup_index = self._get_node_to_backup_index()
        value = key.value_bytes

        placement = m3_placement_pb2.Placement()
        placement.ParseFromString(value)

        for idx, node, pnode in zip(node_to_backup_index, self.nodes, self.plugin_config.placement_nodes):
            if idx is None:
                continue
            src_pnode = self.plugin_manifest.placement_nodes[idx]
            rewrite_single_m3_placement(placement, src_pnode=src_pnode, dst_isolation_group=node.az, dst_pnode=pnode)
        key.set_value_bytes(placement.SerializeToString())

    async def step_rewrite_etcd(self):
        etcd = self.plugin_manifest.etcd.copy(deep=True)
        for prefix in etcd.prefixes:
            for key in prefix.keys:
                key_bytes = key.key_bytes
                if key_bytes.startswith(b"_sd.placement/") and key_bytes.endswith(b"/m3db"):
                    self._rewrite_m3db_placement(key)
        return etcd

    async def step_restore_etcd(self):
        return await self.restore_etcd_dump(self.result_rewrite_etcd)


plugin_info = {"backup": M3DBBackupOp, "manifest": M3DBManifest, "restore": M3DRestoreOp, "config": M3DBConfiguration}
