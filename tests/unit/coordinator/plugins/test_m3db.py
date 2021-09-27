"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Test that the plugin m3 specific flow (backup + restore) works

"""
from ..conftest import COORDINATOR_NODES
from abc import ABC
from astacus.common import ipc
from astacus.common.asyncstorage import AsyncJsonStorage
from astacus.common.etcd import b64encode_to_str
from astacus.common.storage import JsonStorage
from astacus.coordinator.config import CoordinatorConfig
from astacus.coordinator.plugins import base, m3db
from astacus.coordinator.state import CoordinatorState
from dataclasses import dataclass
from tests.unit.common.test_m3placement import create_dummy_placement
from typing import Optional

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
        self.state = CoordinatorState()


BACKUP_FAILS = [0, 1, None]

KEY1_B64 = b64encode_to_str(f"_sd.placement/{ENV}/m3db".encode())
KEY2_B64 = b64encode_to_str(b"key2")
VALUE1_B64 = b64encode_to_str(create_dummy_placement().SerializeToString())
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
        op.state.shutting_down = fail_at == 0
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
            ]},
            status_code=200 if fail_at != 1 else 500,
        )
        assert await op.try_run() == (fail_at is None)
    if fail_at is not None:
        return
    assert op.plugin_data == PLUGIN_DATA


class DummyJsonStorage(JsonStorage, ABC):
    def __init__(self, expected_name: str):
        self.expected_name = expected_name

    def download_json(self, name: str):
        assert self.expected_name == name
        return {
            "plugin": "m3db",
            "plugin_data": PLUGIN_DATA,
            "attempt": 1,
            "snapshot_results": [],
            "start": "2020-01-01 12:00",
            "upload_results": [],
        }


class DummyM3DRestoreOp(m3db.M3DRestoreOp):
    nodes = COORDINATOR_NODES
    steps = ["init", "backup_manifest", "rewrite_etcd", "restore_etcd"]

    def __init__(self, *, partial):
        # pylint: disable=super-init-not-called
        self.config = CoordinatorConfig.parse_obj(COORDINATOR_CONFIG)
        self.state = CoordinatorState()
        req = ipc.RestoreRequest()
        if partial:
            req.partial_restore_nodes = [ipc.PartialRestoreRequestNode(backup_index=0, node_index=0)]
        self.req = req
        self.json_storage = AsyncJsonStorage(DummyJsonStorage(self.result_backup_name))

    result_backup_name = "x"


@dataclass
class RestoreTest:
    fail_at: Optional[int] = None
    partial: bool = False


@pytest.mark.asyncio
@pytest.mark.parametrize("rt", [RestoreTest(fail_at=i) for i in range(3)] + [RestoreTest(), RestoreTest(partial=True)])
async def test_m3_restore(rt):
    fail_at = rt.fail_at
    op = DummyM3DRestoreOp(partial=rt.partial)
    with respx.mock:
        op.state.shutting_down = fail_at == 0
        respx.post("http://dummy/etcd/kv/deleterange", content={"ok": True}, status_code=200 if fail_at != 1 else 500)
        respx.post("http://dummy/etcd/kv/put", content={"ok": True}, status_code=200 if fail_at != 2 else 500)
        assert await op.try_run() == (fail_at is None)
