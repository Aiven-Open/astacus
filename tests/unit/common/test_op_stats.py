"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Test stats sending.

"""
from astacus.common.op import Op
from astacus.common.statsd import StatsClient
from astacus.coordinator.cluster import Cluster
from astacus.coordinator.plugins.base import OpBase
from astacus.coordinator.state import CoordinatorState
from unittest.mock import patch

import pytest


class DummyOp(OpBase):
    def __init__(self):
        # pylint: disable=super-init-not-called
        self.op_id = 1
        self.steps = ["one", "two", "three"]
        self.stats = StatsClient(config=None)
        self.info = Op.Info(op_id=1)
        self.state = CoordinatorState()

    async def step_one(self, cluster: Cluster):
        return True

    async def step_two(self, cluster: Cluster):
        return True

    async def step_three(self, cluster: Cluster):
        return True


@pytest.mark.asyncio
async def test_op_stats():
    op = DummyOp()
    with patch.object(StatsClient, "timing") as mock_stats_timing:
        await op.try_run(Cluster(nodes=[]))
    assert mock_stats_timing.call_count == 3


def test_status_fail_stats():
    op = DummyOp()
    with patch.object(StatsClient, "increase") as mock_stats_counter:
        op.set_status_fail()
    mock_stats_counter.assert_called_with("astacus_fail", tags={"op": "DummyOp"})
