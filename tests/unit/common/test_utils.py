"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Test astacus.common.utils

"""

from astacus.common import utils

import asyncio
import logging
import pytest
import time

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_exponential_backoff(mocker):
    _waits = []
    base = 42

    def _time_monotonic():
        elapsed = sum(_waits)
        logger.debug("_time_monotonic: %s elapsed", elapsed)
        return base + elapsed

    mocker.patch.object(time, "monotonic", new=_time_monotonic)
    mocker.patch.object(time, "sleep", new=_waits.append)

    async def _asleep(d):
        _waits.append(d)

    mocker.patch.object(asyncio, "sleep", new=_asleep)
    assert list(utils.exponential_backoff(initial=1, retries=5)) == list(range(6))

    def _assert_rounded_waits_equals(x):
        assert len(_waits) == len(x)
        assert [round(w) for w in _waits] == x
        _waits.clear()

    _assert_rounded_waits_equals([1, 2, 4, 8, 16])

    list(utils.exponential_backoff(initial=1, duration=30))
    # 1+2+4+8 = 15; +16 = 31 => not within 30s
    _assert_rounded_waits_equals([1, 2, 4, 8])

    # Ensure the async version works too
    async for _ in utils.exponential_backoff(initial=1, retries=5):
        pass
    _assert_rounded_waits_equals([1, 2, 4, 8, 16])
