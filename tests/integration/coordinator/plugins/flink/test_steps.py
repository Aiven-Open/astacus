"""
Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""

from astacus.coordinator.plugins.base import StepsContext
from astacus.coordinator.plugins.flink.manifest import FlinkManifest
from astacus.coordinator.plugins.flink.steps import FlinkManifestStep, RestoreDataStep, RetrieveDataStep
from astacus.coordinator.plugins.zookeeper import KazooZooKeeperClient
from unittest.mock import Mock
from uuid import uuid4


async def test_restore_data(zookeeper_client: KazooZooKeeperClient) -> None:
    table_id1 = str(uuid4()).partition("-")[0]
    table_id2 = str(uuid4()).partition("-")[0]
    data = {
        "catalog": {
            "table_names": {
                "test": table_id1,
                "test2": table_id2,
            },
            "table_ids": {
                table_id1: "json",
                table_id2: "json2",
            },
        },
        "flink": {
            "test": {
                "test_node": "test_value",
            },
        },
    }
    manifest = FlinkManifest(data=data)
    context = StepsContext()
    context.set_result(FlinkManifestStep, manifest)
    await RestoreDataStep(zookeeper_client, ["catalog", "flink"]).run_step(cluster=Mock(), context=context)
    res = await RetrieveDataStep(zookeeper_client, ["catalog", "flink"]).run_step(cluster=Mock(), context=Mock())
    assert res == data
