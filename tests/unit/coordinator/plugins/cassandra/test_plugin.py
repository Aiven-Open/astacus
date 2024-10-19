"""Copyright (c) 2022 Aiven Ltd
See LICENSE for details.
"""

from astacus.common import ipc
from astacus.coordinator.cluster import Cluster
from astacus.coordinator.plugins.base import StepFailedError, StepsContext
from astacus.coordinator.plugins.cassandra import plugin
from astacus.coordinator.plugins.cassandra.model import CassandraConfigurationNode
from pytest_mock import MockerFixture
from tests.unit.conftest import CassandraTestConfig
from unittest.mock import Mock

import pytest


@pytest.fixture(name="cplugin")
def fixture_cplugin(cassandra_test_config: CassandraTestConfig) -> plugin.CassandraPlugin:
    return plugin.CassandraPlugin(
        client=cassandra_test_config.cassandra_client_config,
        nodes=[CassandraConfigurationNode(listen_address="127.0.0.1")],
    )


async def test_step_cassandrasubop(mocker: MockerFixture) -> None:
    mocker.patch.object(plugin, "run_subop")

    step = plugin.CassandraSubOpStep(op=ipc.CassandraSubOp.stop_cassandra)
    cluster = Cluster(nodes=[])
    context = StepsContext()
    await step.run_step(cluster, context)


@pytest.mark.parametrize("success", [False, True])
async def test_step_cassandra_validate_configuration(mocker: MockerFixture, success: bool) -> None:
    step = plugin.ValidateConfigurationStep(nodes=[])
    context = Mock()
    if success:
        cluster = Mock(nodes=[])
        await step.run_step(cluster, context)
    else:
        # node count mismatch
        cluster = Mock(nodes=[42])
        with pytest.raises(StepFailedError):
            await step.run_step(cluster, context)


def test_get_backup_steps(mocker: MockerFixture, cplugin):
    context = mocker.Mock()
    cplugin.get_backup_steps(context=context)


def test_get_restore_steps(mocker: MockerFixture, cplugin):
    context = mocker.Mock()
    cplugin.get_restore_steps(context=context, req=ipc.RestoreRequest())
