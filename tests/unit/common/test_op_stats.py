"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Test stats sending.

"""
from astacus.common.statsd import StatsClient
from astacus.coordinator.plugins.base import OpBase
from unittest.mock import patch

import pytest


class DummyOp(OpBase):
    def __init__(self):
        # pylint: disable=super-init-not-called
        self.steps = ["one", "two", "three"]
        self.stats = StatsClient(config=None)

    async def step_one(self):
        return True

    async def step_two(self):
        return True

    async def step_three(self):
        return True


@pytest.mark.asyncio
async def test_op_stats():
    op = DummyOp()
    with patch.object(StatsClient, "timing") as mock_stats_timing:
        await op.try_run()
    assert mock_stats_timing.call_count == 3
