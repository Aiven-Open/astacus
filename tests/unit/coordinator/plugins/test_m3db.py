"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Test that the plugin m3 specific flow (backup + restore) works

"""

from astacus.common import ipc
from astacus.common.etcd import b64encode_to_str, ETCDClient
from astacus.common.statsd import StatsClient
from astacus.coordinator.config import CoordinatorConfig
from astacus.coordinator.coordinator import Coordinator, SteppedCoordinatorOp
from astacus.coordinator.plugins import m3db
from astacus.coordinator.plugins.base import BackupManifestStep, MapNodesStep, StepsContext
from astacus.coordinator.plugins.m3db import (
    get_etcd_prefixes,
    InitStep,
    M3DBPlugin,
    PrepareM3ManifestStep,
    RestoreEtcdStep,
    RetrieveEtcdAgainStep,
    RetrieveEtcdStep,
    RewriteEtcdStep,
)
from astacus.coordinator.state import CoordinatorState
from collections.abc import Sequence
from dataclasses import dataclass
from starlette.background import BackgroundTasks
from starlette.datastructures import URL
from tests.unit.common.test_m3placement import create_dummy_placement
from unittest.mock import Mock

import datetime
import pytest
import respx

ENV = "dummyenv"

PLACEMENT_NODES = [
    {"node_id": "node-1", "hostname": "node-1", "endpoint": "http://node-1:123456"},
    {"node_id": "node-2", "hostname": "node-2", "endpoint": "http://node-2:123456"},
]

COORDINATOR_CONFIG = {
    "plugin": "m3db",
    "plugin_config": {
        "etcd_url": "http://dummy/etcd",
        "environment": ENV,
        "placement_nodes": PLACEMENT_NODES,
    },
    "nodes": [{"url": "http://localhost:12345/asdf"}, {"url": "http://localhost:12346/asdf"}],
}

BACKUP_FAILS = [0, 1, None]

KEY1_B64 = b64encode_to_str(f"_sd.placement/{ENV}/m3db".encode())
KEY2_B64 = b64encode_to_str(b"key2")
VALUE1_B64 = b64encode_to_str(create_dummy_placement().SerializeToString())
VALUE2_B64 = b64encode_to_str(b"value2")

PREFIXES = [
    {
        "keys": [
            {"key_b64": KEY1_B64, "value_b64": VALUE1_B64},
            {"key_b64": KEY2_B64, "value_b64": VALUE2_B64},
        ],
        "prefix_b64": b64encode_to_str(prefix.format(env=ENV).encode()),
    }
    for prefix in m3db.ETCD_PREFIX_FORMATS
]

PLUGIN_DATA = {"etcd": {"prefixes": PREFIXES}, "placement_nodes": PLACEMENT_NODES}


@pytest.fixture(name="coordinator")
def fixture_coordinator() -> Coordinator:
    return Coordinator(
        request_url=URL("/"),
        background_tasks=BackgroundTasks(),
        config=CoordinatorConfig.parse_obj(COORDINATOR_CONFIG),
        state=CoordinatorState(),
        stats=StatsClient(config=None),
        storage_factory=Mock(),
    )


@pytest.fixture(name="plugin")
def fixture_plugin(coordinator: Coordinator) -> M3DBPlugin:
    return M3DBPlugin.parse_obj(coordinator.config.plugin_config)


@pytest.fixture(name="etcd_client")
def fixture_etcd_client(plugin: M3DBPlugin) -> ETCDClient:
    return ETCDClient(plugin.etcd_url)


@pytest.mark.parametrize("fail_at", BACKUP_FAILS)
async def test_m3_backup(coordinator: Coordinator, plugin: M3DBPlugin, etcd_client: ETCDClient, fail_at: int | None):
    etcd_prefixes = get_etcd_prefixes(plugin.environment)
    op = SteppedCoordinatorOp(
        c=coordinator,
        attempts=1,
        steps=[
            InitStep(placement_nodes=plugin.placement_nodes),
            RetrieveEtcdStep(etcd_client=etcd_client, etcd_prefixes=etcd_prefixes),
            RetrieveEtcdAgainStep(etcd_client=etcd_client, etcd_prefixes=etcd_prefixes),
            PrepareM3ManifestStep(placement_nodes=plugin.placement_nodes),
        ],
        operation_context=Mock(),
    )
    context = StepsContext()
    with respx.mock:
        op.state.shutting_down = fail_at == 0
        respx.post("http://dummy/etcd/kv/range").respond(
            json={
                "kvs": [
                    {"key": KEY1_B64, "value": VALUE1_B64},
                    {"key": KEY2_B64, "value": VALUE2_B64},
                ]
            },
            status_code=200 if fail_at != 1 else 500,
        )
        assert await op.try_run(op.get_cluster(), context) == (fail_at is None)
    if fail_at is not None:
        return
    assert context.get_result(PrepareM3ManifestStep) == PLUGIN_DATA


@dataclass
class RestoreTest:
    fail_at: int | None = None
    partial: bool = False


@pytest.mark.parametrize("rt", [RestoreTest(fail_at=i) for i in range(3)] + [RestoreTest()])
async def test_m3_restore(coordinator: Coordinator, plugin: M3DBPlugin, etcd_client: ETCDClient, rt: RestoreTest) -> None:
    partial_restore_nodes: Sequence[ipc.PartialRestoreRequestNode] | None = None
    if rt.partial:
        partial_restore_nodes = [ipc.PartialRestoreRequestNode(backup_index=0, node_index=0)]
    op = SteppedCoordinatorOp(
        c=coordinator,
        attempts=1,
        steps=[
            MapNodesStep(partial_restore_nodes=partial_restore_nodes),
            RewriteEtcdStep(placement_nodes=plugin.placement_nodes, partial_restore_nodes=partial_restore_nodes),
            RestoreEtcdStep(etcd_client=etcd_client, partial_restore_nodes=partial_restore_nodes),
        ],
        operation_context=Mock(),
    )
    context = StepsContext()
    context.set_result(
        BackupManifestStep,
        ipc.BackupManifest(
            plugin=ipc.Plugin.m3db,
            plugin_data=PLUGIN_DATA,
            attempt=1,
            snapshot_results=[],
            start=datetime.datetime(2020, 1, 1, 12, 00, tzinfo=datetime.UTC),
            upload_results=[],
        ),
    )
    fail_at = rt.fail_at
    with respx.mock:
        op.state.shutting_down = fail_at == 0
        respx.post("http://dummy/etcd/kv/deleterange").respond(json={"ok": True}, status_code=200 if fail_at != 1 else 500)
        respx.post("http://dummy/etcd/kv/put").respond(json={"ok": True}, status_code=200 if fail_at != 2 else 500)
        assert await op.try_run(op.get_cluster(), context) == (fail_at is None)
