"""
Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""

from astacus.common import ipc
from astacus.common.cassandra.schema import CassandraSchema
from astacus.coordinator.cluster import Cluster
from astacus.coordinator.config import CoordinatorNode
from astacus.coordinator.plugins import base
from astacus.coordinator.plugins.cassandra import restore_steps
from astacus.coordinator.plugins.cassandra.model import CassandraConfigurationNode, CassandraManifest, CassandraManifestNode
from collections.abc import Sequence
from pytest_mock import MockerFixture
from tests.unit.coordinator.plugins.cassandra.builders import build_keyspace
from unittest.mock import Mock, patch
from uuid import UUID

import datetime
import pytest

# TBD: Eventually multinode configuration would be perhaps interesting to test too


def _manifest_node(node_index: int) -> CassandraManifestNode:
    return CassandraManifestNode(
        address=f"127.0.0.{node_index}",
        host_id=UUID(int=node_index),
        listen_address=f"::{node_index}",
        rack=f"r{node_index}",
        tokens=[f"token{node_index}"],
    )


def _configuration_node(node_index: int) -> CassandraConfigurationNode:
    return CassandraConfigurationNode(
        address=f"127.0.0.{node_index}",
        host_id=UUID(int=node_index),
        listen_address=f"::{node_index}",
        tokens=[f"token{node_index}"],
    )


def _coordinator_node(node_index: int) -> CoordinatorNode:
    return CoordinatorNode(
        url=f"http:://localhost:{node_index}",
        az=f"az-{node_index}",
    )


_pr_node = ipc.PartialRestoreRequestNode


@pytest.mark.parametrize("override_tokens", [False, True])
@pytest.mark.parametrize("replace_backup_nodes", [False, True])
async def test_step_start_cassandra(mocker: MockerFixture, override_tokens: bool, replace_backup_nodes: bool) -> None:
    plugin_manifest = CassandraManifest(
        cassandra_schema=CassandraSchema(keyspaces=[]),
        nodes=[_manifest_node(1)],
    )

    backup_manifest = ipc.BackupManifest(
        start=datetime.datetime.now(),
        attempt=1,
        snapshot_results=[ipc.SnapshotResult()],
        upload_results=[],
        plugin=ipc.Plugin.cassandra,
        plugin_data=plugin_manifest.dict(),
    )

    nodes = [_coordinator_node(1)]
    node_to_backup_index = [0]

    def get_result(cl):
        match cl:
            case base.BackupManifestStep:
                return backup_manifest
            case restore_steps.ParsePluginManifestStep:
                return plugin_manifest
            case base.MapNodesStep:
                return node_to_backup_index
            case _:
                raise NotImplementedError(cl)

    expected_reqs = [
        ipc.CassandraStartRequest(
            tokens=["token1"] if override_tokens else None,
            replace_address_first_boot="::1" if replace_backup_nodes else None,
            skip_bootstrap_streaming=True if replace_backup_nodes else None,
        )
    ]

    run_subop = mocker.patch.object(restore_steps, "run_subop")

    step = restore_steps.StartCassandraStep(
        replace_backup_nodes=replace_backup_nodes, override_tokens=override_tokens, cassandra_nodes=[_configuration_node(1)]
    )
    context = base.StepsContext()
    mocker.patch.object(context, "get_result", new=get_result)
    cluster = Cluster(nodes=nodes)
    await step.run_step(cluster, context)
    run_subop.assert_awaited_once_with(
        cluster,
        ipc.CassandraSubOp.start_cassandra,
        nodes=nodes,
        reqs=expected_reqs,
        result_class=ipc.NodeResult,
    )


async def test_step_stop_replaced_nodes(mocker: MockerFixture) -> None:
    # Node 3 is replacing node 1.
    manifest_nodes = [_manifest_node(1), _manifest_node(2)]
    cassandra_nodes = [_configuration_node(1), _configuration_node(2), _configuration_node(3)]
    nodes = [_coordinator_node(1), _coordinator_node(2), _coordinator_node(3)]
    node_to_backup_index = [None, None, 0]
    partial_restore_nodes = [_pr_node(backup_index=0, node_index=2)]

    plugin_manifest = CassandraManifest(cassandra_schema=CassandraSchema(keyspaces=[]), nodes=manifest_nodes)

    def get_result(cl):
        match cl:
            case restore_steps.ParsePluginManifestStep:
                return plugin_manifest
            case base.MapNodesStep:
                return node_to_backup_index
            case _:
                raise NotImplementedError(cl)

    run_subop = mocker.patch.object(restore_steps, "run_subop")

    step = restore_steps.StopReplacedNodesStep(partial_restore_nodes=partial_restore_nodes, cassandra_nodes=cassandra_nodes)
    context = Mock(get_result=get_result)
    cluster = Mock(nodes=nodes)
    result = await step.run_step(cluster, context)
    assert result == [_coordinator_node(1)]
    run_subop.assert_awaited_once_with(
        cluster,
        ipc.CassandraSubOp.stop_cassandra,
        nodes=[_coordinator_node(1)],
        result_class=ipc.NodeResult,
    )


class AsyncIterableWrapper:
    def __init__(self, iterable):
        self.data = list(iterable)
        self.index = -1

    def __aiter__(self):
        return self

    async def __anext__(self):
        self.index += 1
        if self.index >= len(self.data):
            raise StopAsyncIteration
        return self.data[self.index]


@pytest.mark.parametrize("steps,success", [([True], True), ([False, True], True), ([False], False)])
async def test_step_wait_cassandra_up(mocker: MockerFixture, steps: list[bool], success: bool) -> None:
    get_schema_steps = steps[:]

    async def get_schema_hash(cluster, nodes):
        assert get_schema_steps
        return get_schema_steps.pop(0), "unused-error"

    mocker.patch.object(restore_steps, "get_schema_hash", new=get_schema_hash)

    mocker.patch.object(
        restore_steps.utils,  # type: ignore[attr-defined]
        "exponential_backoff",
        return_value=AsyncIterableWrapper(steps),
    )

    step = restore_steps.WaitCassandraUpStep(duration=123, replaced_node_step=None)
    context = base.StepsContext()
    stopped_nodes = [_coordinator_node(1)]
    context.set_result(restore_steps.StopReplacedNodesStep, stopped_nodes)
    cluster = Cluster(nodes=stopped_nodes + [_coordinator_node(2)])
    if success:
        await step.run_step(cluster, context)
    else:
        with pytest.raises(base.StepFailedError):
            await step.run_step(cluster, context)


@pytest.mark.parametrize(
    "replaced_node_step, expected_nodes", [(restore_steps.StopReplacedNodesStep, [_coordinator_node(2)]), (None, None)]
)
async def test_stopped_nodes_for_wait_cassandra_up_step(
    replaced_node_step: type[restore_steps.StopReplacedNodesStep] | None,
    expected_nodes: Sequence[CoordinatorNode] | None,
    context: base.StepsContext,
) -> None:
    cluster = Cluster(nodes=[_coordinator_node(2)])

    if replaced_node_step:
        stopped_nodes = [_coordinator_node(1)]
        context.set_result(replaced_node_step, stopped_nodes)
        cluster.nodes = [*cluster.nodes, *stopped_nodes]

    step = restore_steps.WaitCassandraUpStep(duration=123, replaced_node_step=replaced_node_step)

    with patch(
        "astacus.coordinator.plugins.cassandra.restore_steps.get_schema_hash", return_value=("hash", "")
    ) as mock_get_schema_hash:
        await step.run_step(cluster, context)
        mock_get_schema_hash.assert_called_once_with(cluster=cluster, nodes=expected_nodes)


def test_rewrite_datacenters() -> None:
    pre_rewrite_cql = "create me, please"
    keyspaces = [
        build_keyspace("remains_unchanged").with_cql_create_self(pre_rewrite_cql).with_network_topology_strategy_dcs({}),
        build_keyspace("needs_rewrite")
        .with_cql_create_self(pre_rewrite_cql)
        .with_network_topology_strategy_dcs({"new_dc": "3"}),
    ]
    restore_steps._rewrite_datacenters(keyspaces)  # pylint: disable=protected-access
    unchanged_keyspace, rewritten_keyspace = keyspaces[0], keyspaces[1]
    assert unchanged_keyspace.cql_create_self == pre_rewrite_cql
    assert "'new_dc': '3'" in rewritten_keyspace.cql_create_self
