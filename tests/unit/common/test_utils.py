"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Test astacus.common.utils

"""

from astacus.common import utils
from datetime import timedelta

import asyncio
import logging
import pytest
import tempfile
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
    retries = list(utils.exponential_backoff(initial=1, retries=5))
    assert retries == list(range(6))

    def _assert_rounded_waits_equals(x):
        assert len(_waits) == len(x)
        assert [round(w) for w in _waits] == x
        _waits.clear()

    _assert_rounded_waits_equals([1, 2, 4, 8, 16])

    list(utils.exponential_backoff(initial=1, duration=30))
    # 1+2+4+8 = 15; +16 = 31 => not within 30s
    _assert_rounded_waits_equals([1, 2, 4, 8])

    # Ensure the async version works too
    retries = []
    async for retry in utils.exponential_backoff(initial=1, retries=5):
        retries.append(retry)
    assert retries == list(range(6))
    _assert_rounded_waits_equals([1, 2, 4, 8, 16])


@pytest.mark.parametrize(
    "v,s", [
        (timedelta(days=1, seconds=1), "1d 1s"),
        (timedelta(hours=3, minutes=2, seconds=1), "3h 2m 1s"),
        (timedelta(seconds=0), ""),
    ]
)
def test_timedelta_as_short_str(v, s):
    assert utils.timedelta_as_short_str(v) == s


@pytest.mark.parametrize(
    "v,s", [
        (1_234_567_901_234_678, "1234.6 TB"),
        (1_234_567_901_234, "1.2 TB"),
        (1_234_567_901, "1.2 GB"),
        (1_234_567, "1.2 MB"),
        (1_234, "1.2 KB"),
        (1, "1 B"),
    ]
)
def test_size_as_short_str(v, s):
    assert utils.size_as_short_str(v) == s


def test_sizelimitedfile():
    with tempfile.NamedTemporaryFile() as f:
        f.write(b"foobarbaz")
        f.flush()
        lf = utils.SizeLimitedFile(path=f.name, file_size=6)
        assert lf.read() == b"foobar"
        assert lf.tell() == 6
        assert lf.seek(0, 0) == 0
        assert lf.tell() == 0
        assert lf.read() == b"foobar"
        assert lf.seek(0, 2) == 6
        assert not lf.read()
        assert lf.seek(3, 0) == 3
        assert lf.read() == b"bar"
