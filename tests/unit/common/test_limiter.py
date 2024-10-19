"""Copyright (c) 2021 Aiven Ltd
See LICENSE for details.
"""

from astacus.common.limiter import gather_limited, Limiter
from collections.abc import Sequence

import asyncio
import pytest


@pytest.mark.parametrize(
    "limit,expected_trace",
    [
        (1, ["s1", "e1", "s2", "e2", "s3", "e3"]),
        (2, ["s1", "s2", "e2", "s3", "e3", "e1"]),
        (3, ["s1", "s2", "s3", "e2", "e3", "e1"]),
    ],
)
async def test_limiter(limit: int, expected_trace: Sequence[str]) -> None:
    trace = []

    async def add_trace(start: str, sleep: float, stop: str):
        trace.append(start)
        await asyncio.sleep(sleep)
        trace.append(stop)

    limiter = Limiter(limit)
    await asyncio.gather(
        limiter.run(add_trace("s1", 0.1, "e1")),
        limiter.run(add_trace("s2", 0.01, "e2")),
        limiter.run(add_trace("s3", 0.03, "e3")),
    )
    assert trace == expected_trace


@pytest.mark.parametrize(
    "limit,expected_trace",
    [
        (1, ["s1", "e1", "s2", "e2", "s3", "e3"]),
        (2, ["s1", "s2", "e2", "s3", "e3", "e1"]),
        (3, ["s1", "s2", "s3", "e2", "e3", "e1"]),
    ],
)
async def test_gather_limited(limit: int, expected_trace: Sequence[str]) -> None:
    trace = []

    async def add_trace(start: str, sleep: float, stop: str):
        trace.append(start)
        await asyncio.sleep(sleep)
        trace.append(stop)

    await gather_limited(
        limit,
        [
            add_trace("s1", 0.1, "e1"),
            add_trace("s2", 0.01, "e2"),
            add_trace("s3", 0.03, "e3"),
        ],
    )
    assert trace == expected_trace
