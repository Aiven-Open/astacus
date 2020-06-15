"""Copyright (c) 2020 Aiven Ltd
See LICENSE for details

m3db backup/restore plugin

All of the actual heavy lifting is done using the base file
snapshot/restore functionality. m3db plugin will simply ensure etcd
state is consistent.

Note that the rewriting of etcd content is pretty hacky; instead of
dealing with the pretty heavy protobuf stuff, we simply do binary
rewrites of the specific m3db content. The relevant bits to know here
are that each entry protobuf contains ( see
https://developers.google.com/protocol-buffers/docs/encoding ) :

  <wire-type: 3 bits> <field-number : varint>

and then wire-type specific data, notably:

  <wire-type = 2: length-delimited> <field-number> =>

  <length : varint> (probably just one byte)
  <bytes>: #length bytes

We make the optimistic assumption that there are no false positives
for the (relatively long) (wiretype,fieldnumber,fieldlength,fielddata)
tuple. Due to how the encoding works, ASCII text is unlikely to be
mistaken for varints as (large) varints have highest bit set.

Probably option to (detect/)skip this altogether would make sense at
some point as well, as if cluster has static hostnames, no rewrites
are needed anyway.

"""

from .etcd import ETCDBackupOpBase, ETCDConfiguration, ETCDDump, ETCDRestoreOpBase
from astacus.common import exceptions, ipc
from astacus.common.utils import AstacusModel
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


def protobuf_lv(b, *, prefix=b""):
    """ protobuf length-value encoding """
    if isinstance(b, str):
        b = b.encode()
    assert isinstance(b, bytes)
    assert len(b) < MAXIMUM_PROTOBUF_STR_LENGTH  # we don't support real varints
    return prefix + bytes([len(b)]) + b


def protobuf_tlv(t, b):
    """ protobuf type-length-value encoding """
    return protobuf_lv(b, prefix=bytes([2 + (t << 3)]))


def rewrite_single_m3_placement(
    value, *, src_pnode: M3PlacementNode, dst_pnode: M3PlacementNode, src_node, dst_node, ensure_all=True
):
    """ rewrite single m3 placement entry in-place in (binary) protobuf

Relevant places ( see m3db src/cluster/generated/proto/placementpb/placement.proto ) :

instance<str,Instance> map = 1, and then

message Instance {
  string id                 = 1;
  string isolation_group    = 2;
  string endpoint           = 5;
  string hostname           = 8;
}
"""
    ovalue = value

    def _replace(src, dst, what):
        if src == dst:
            return value
        replaced_value = value.replace(src, dst)
        logger.debug("Replacing %s %r with %r", what, src, dst)
        if ensure_all and replaced_value == value:
            raise ValueError(f"{what}, expected to be {src!r} missing from placement plan {ovalue!r}")
        return replaced_value

    # Be bit more brute-force than strictly necessary; just
    # replace 'id' in general, which takes care of both id key
    # as well key in instance map.
    value = _replace(protobuf_lv(src_pnode.node_id), protobuf_lv(dst_pnode.node_id), "node_id")

    # For everything else, replace more conservatively by
    # including also the field ids
    value = _replace(protobuf_tlv(2, src_node.az), protobuf_tlv(2, dst_node.az), "az")
    for t, field in [(5, "endpoint"), (8, "hostname")]:
        old_raw = getattr(src_pnode, field)
        if field == "hostname" and old_raw == src_pnode.node_id:
            # node_id was already globally replaced
            continue
        old_value = protobuf_tlv(t, old_raw)
        new_value = protobuf_tlv(t, getattr(dst_pnode, field))
        value = _replace(old_value, new_value, field)
    return value


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

        for idx, node, pnode in zip(node_to_backup_index, self.nodes, self.plugin_config.placement_nodes):
            if idx is None:
                continue
            src_pnode = self.plugin_manifest.placement_nodes[idx]

            # not technically node, but has az, which is enough ( and
            # hostname, but it is derived potentially differently from
            # 'configured' value in etcd so it is nbot used )
            src_node = self.result_backup_manifest.snapshot_results[idx]

            value = rewrite_single_m3_placement(
                value, src_pnode=src_pnode, src_node=src_node, dst_node=node, dst_pnode=pnode
            )
        key.set_value_bytes(value)

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
