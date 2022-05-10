"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Test stats sending.

"""
from astacus.common import op
from astacus.common.ipc import Plugin
from astacus.common.statsd import StatsClient
from astacus.common.storage import MultiStorage
from astacus.coordinator.cluster import Cluster
from astacus.coordinator.config import CoordinatorConfig
from astacus.coordinator.coordinator import Coordinator, SteppedCoordinatorOp
from astacus.coordinator.plugins.base import Step, StepsContext
from astacus.coordinator.state import CoordinatorState
from fastapi import BackgroundTasks
from starlette.datastructures import URL
from unittest.mock import patch

import pytest
import threading


class DummyStep(Step[bool]):
    async def run_step(self, cluster: Cluster, context: StepsContext) -> bool:
        return True


class DummyStep1(DummyStep):
    pass


class DummyStep2(DummyStep):
    pass


class DummyStep3(DummyStep):
    pass


@pytest.mark.asyncio
async def test_op_stats():
    stats = StatsClient(config=None)
    coordinator = Coordinator(
        request_url=URL(),
        background_tasks=BackgroundTasks(),
        config=CoordinatorConfig(plugin=Plugin.files),
        state=CoordinatorState(),
        stats=stats,
        sync_lock=threading.RLock(),
        hexdigest_mstorage=MultiStorage(),
        json_mstorage=MultiStorage(),
    )
    operation = SteppedCoordinatorOp(
        c=coordinator,
        attempts=1,
        steps=[
            DummyStep1(),
            DummyStep2(),
            DummyStep3(),
        ],
    )
    operation.op_id = operation.info.op_id
    operation.stats = stats
    cluster = Cluster(nodes=[])
    context = StepsContext()
    with patch.object(stats, "timing") as mock_stats_timing:
        await operation.try_run(cluster, context)
    assert mock_stats_timing.call_count == 3


class DummyOp(op.Op):
    pass


def test_status_fail_stats():
    stats = StatsClient(config=None)
    operation = DummyOp(info=op.Op.Info(op_id=1, op_name="DummyOp"), op_id=1, stats=stats)

    with patch.object(stats, "increase") as mock_stats_counter:
        operation.set_status_fail()
    mock_stats_counter.assert_called_with("astacus_fail", tags={"op": "DummyOp"})
