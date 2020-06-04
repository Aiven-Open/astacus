"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Test that the plugin m3 specific flow (backup + restore) works

"""
# pylint: disable=too-many-ancestors

from ..conftest import COORDINATOR_NODES
from astacus.common import ipc
from astacus.common.etcd import b64encode_to_str
from astacus.coordinator.config import CoordinatorConfig, CoordinatorNode
from astacus.coordinator.plugins import base, m3db

import pytest
import respx

ENV = "dummyenv"

PLACEMENT_NODES = [
    {
        "node_id": "node-1",
        "hostname": "node-1",
        "endpoint": "http://node-1:123456"
    },
    {
        "node_id": "node-2",
        "hostname": "node-2",
        "endpoint": "http://node-2:123456"
    },
]

COORDINATOR_CONFIG = {
    "plugin": "m3db",
    "plugin_config": {
        "etcd_url": "http://dummy/etcd",
        "environment": ENV,
        "placement_nodes": PLACEMENT_NODES,
    },
}


class DummyM3DBBackupOp(m3db.M3DBBackupOp):
    nodes = COORDINATOR_NODES

    def __init__(self):
        # pylint: disable=super-init-not-called
        self.config = CoordinatorConfig.parse_obj(COORDINATOR_CONFIG)
        self.steps = [step for step in self.steps if getattr(base.BackupOpBase, f"step_{step}", None) is None]


BACKUP_FAILS = [1, None]

KEY1_B64 = b64encode_to_str(f"_sd.placement/{ENV}/m3db".encode())
KEY2_B64 = b64encode_to_str(b"key2")
VALUE1_B64 = b64encode_to_str(b"value1")
VALUE2_B64 = b64encode_to_str(b"value2")

PREFIXES = [{
    "keys": [
        {
            "key_b64": KEY1_B64,
            "value_b64": VALUE1_B64
        },
        {
            "key_b64": KEY2_B64,
            "value_b64": VALUE2_B64
        },
    ],
    "prefix_b64": b64encode_to_str(prefix.format(env=ENV).encode())
} for prefix in DummyM3DBBackupOp.etcd_prefix_formats]

PLUGIN_DATA = {"etcd": {"prefixes": PREFIXES}, "placement_nodes": PLACEMENT_NODES}


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_at", BACKUP_FAILS)
async def test_m3_backup(fail_at):
    op = DummyM3DBBackupOp()
    assert op.steps == ['init', 'retrieve_etcd', 'retrieve_etcd_again', 'create_m3_manifest']
    with respx.mock:
        if fail_at != 1:
            respx.post(
                "http://dummy/etcd/kv/range",
                content={"kvs": [
                    {
                        "key": KEY1_B64,
                        "value": VALUE1_B64
                    },
                    {
                        "key": KEY2_B64,
                        "value": VALUE2_B64
                    },
                ]}
            )
        assert await op.try_run() == (fail_at is None)
    if fail_at:
        return
    assert op.plugin_data == PLUGIN_DATA


class DummyM3DRestoreOp(m3db.M3DRestoreOp):
    nodes = COORDINATOR_NODES
    steps = ["init", "backup_manifest", "rewrite_etcd", "restore_etcd"]

    def __init__(self):
        # pylint: disable=super-init-not-called
        self.config = CoordinatorConfig.parse_obj(COORDINATOR_CONFIG)

    result_backup_name = "x"

    async def download_backup_manifest(self, backup_name):
        assert backup_name == self.result_backup_name
        return ipc.BackupManifest.parse_obj({
            "plugin": "m3db",
            "plugin_data": PLUGIN_DATA,
            "attempt": 1,
            "snapshot_results": [],
            "start": "2020-01-01 12:00",
        })


RESTORE_FAILS = [1, 2, None]


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_at", RESTORE_FAILS)
async def test_m3_restore(fail_at):
    op = DummyM3DRestoreOp()
    with respx.mock:
        if fail_at != 1:
            respx.post("http://dummy/etcd/kv/deleterange", content={"ok": True})
        if fail_at != 2:
            respx.post("http://dummy/etcd/kv/put", content={"ok": True})
        assert await op.try_run() == (fail_at is None)


@pytest.mark.parametrize(
    "bytes_in,bytes_out",
    [
        (b"", b""),
        (b"foo", b"foo"),
        # node-id is replaced everywhere -> one non-covered case is enough
        (m3db.protobuf_tlv(15, "node-id1"), m3db.protobuf_tlv(15, "node-id22")),
        # az is replaced only in isolation_group -> check both
        (m3db.protobuf_tlv(15, "az1"), m3db.protobuf_tlv(15, "az1")),
        (m3db.protobuf_tlv(2, "az1"), m3db.protobuf_tlv(2, "az22")),
        # endpoint is replaced only in endpoint -> check both
        (m3db.protobuf_tlv(15, "endpoint1"), m3db.protobuf_tlv(15, "endpoint1")),
        (m3db.protobuf_tlv(5, "endpoint1"), m3db.protobuf_tlv(5, "endpoint22")),
        # hostname is replaced only in hostname -> check both
        (m3db.protobuf_tlv(15, "hostname1"), m3db.protobuf_tlv(15, "hostname1")),
        (m3db.protobuf_tlv(8, "hostname1"), m3db.protobuf_tlv(8, "hostname22")),
    ]
)
def test_rewrite_single_m3_placement(bytes_in, bytes_out):
    src_pnode = m3db.M3PlacementNode(
        node_id="node-id1",
        endpoint="endpoint1",
        hostname="hostname1",
    )
    dst_pnode = m3db.M3PlacementNode(
        node_id="node-id22",
        endpoint="endpoint22",
        hostname="hostname22",
    )
    src_node = CoordinatorNode(az="az1", url="unused")
    dst_node = CoordinatorNode(az="az22", url="unused")
    got = m3db.rewrite_single_m3_placement(
        bytes_in, src_pnode=src_pnode, dst_pnode=dst_pnode, src_node=src_node, dst_node=dst_node
    )
    assert got == bytes_out
