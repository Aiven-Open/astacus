"""Copyright (c) 2020 Aiven Ltd
See LICENSE for details.

Test astacus.common.utils

"""

from astacus.common import utils
from astacus.common.utils import AsyncSleeper, build_netloc, parse_umask
from datetime import timedelta
from pathlib import Path
from pytest_mock import MockerFixture

import asyncio
import logging
import pytest
import tempfile
import time

logger = logging.getLogger(__name__)


def test_build_netloc_does_not_change_hostname() -> None:
    assert build_netloc("example.org") == "example.org"


def test_build_netloc_does_not_change_ipv4() -> None:
    assert build_netloc("127.0.0.1") == "127.0.0.1"


def test_build_netloc_escapes_ipv6() -> None:
    assert build_netloc("::1") == "[::1]"


def test_build_netloc_adds_port() -> None:
    assert build_netloc("example.org", 1234) == "example.org:1234"


def test_build_netloc_adds_port_to_ipv4() -> None:
    assert build_netloc("127.0.0.1", 1234) == "127.0.0.1:1234"


def test_build_netloc_adds_port_to_ipv6() -> None:
    assert build_netloc("::1", 1234) == "[::1]:1234"


async def test_httpx_request_connect_failure():
    # Known (most likely) unreachable local IP address
    r = await utils.httpx_request("http://127.0.0.42:12345/foo", caller="test")
    assert r is None


async def test_async_sleeper() -> None:
    sleeper = AsyncSleeper()

    async def wait_and_wake():
        await asyncio.sleep(0.1)
        sleeper.wakeup()

    task = asyncio.create_task(wait_and_wake())
    try:
        start_time = time.monotonic()
        await sleeper.sleep(10)
        assert time.monotonic() - start_time < 1.0
    finally:
        task.done()


async def test_exponential_backoff(mocker: MockerFixture) -> None:
    _waits: list[float] = []
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
    "v,s",
    [
        (timedelta(days=1, seconds=1), "1d 1s"),
        (timedelta(hours=3, minutes=2, seconds=1), "3h 2m 1s"),
        (timedelta(seconds=0), ""),
    ],
)
def test_timedelta_as_short_str(v: timedelta, s: str) -> None:
    assert utils.timedelta_as_short_str(v) == s


@pytest.mark.parametrize(
    "v,s",
    [
        (1_234_567_901_234_678, "1234.6 TB"),
        (1_234_567_901_234, "1.2 TB"),
        (1_234_567_901, "1.2 GB"),
        (1_234_567, "1.2 MB"),
        (1_234, "1.2 KB"),
        (1, "1 B"),
    ],
)
def test_size_as_short_str(v: int, s: str) -> None:
    assert utils.size_as_short_str(v) == s


def test_sizelimitedfile() -> None:
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


def test_open_path_with_atomic_rename(tmp_path: Path) -> None:
    # default is bytes
    f1_path = tmp_path / "f1"
    with utils.open_path_with_atomic_rename(f1_path) as f1:
        f1.write(b"test1")
    assert f1_path.read_text() == "test1"

    # text mode requires passing mode flag but should work
    f2_path = tmp_path / "f2"
    with utils.open_path_with_atomic_rename(f2_path, mode="w") as f2:
        f2.write("test2")
    assert f2_path.read_text() == "test2"

    # rewriting should be fine too
    with utils.open_path_with_atomic_rename(f2_path, mode="w") as f2:
        f2.write("test2-new")
    assert f2_path.read_text() == "test2-new"

    # erroneous cases should not produce file at all
    f3_path = tmp_path / "f3"

    class TestException(RuntimeError):
        pass

    with pytest.raises(TestException):
        with utils.open_path_with_atomic_rename(f3_path):
            raise TestException()
    assert not f3_path.exists()


def test_parse_umask() -> None:
    proc_status = """Name:   cat
Umask:  0027
State:  R (running)
"""
    assert parse_umask(proc_status) == 0o027


def test_parse_umask_fallback() -> None:
    proc_status = """Name:   cat
Umask:  POTATO
State:  R (running)
"""
    assert parse_umask(proc_status) == 0o022


class CacheTester:
    def __init__(self, val: int) -> None:
        self.val = val
        self.call_cnt = 0

    @utils.fifo_cache(2)
    def get(self, key: int) -> int:
        self.call_cnt += 1
        return key + self.val


def test_fifo_cache_is_per_object() -> None:
    instance_1 = CacheTester(1)
    instance_2 = CacheTester(2)
    assert instance_1.get(1) == 2
    # per object cache
    assert instance_2.get(1) == 3


def test_fifo_cache_eviction() -> None:
    instance = CacheTester(0)
    assert instance.get(1) == 1
    assert instance.call_cnt == 1
    assert instance.get(2) == 2
    assert instance.call_cnt == 2
    assert instance.get(3) == 3
    assert instance.call_cnt == 3
    # key=1 was evicted
    assert instance.get(1) == 1
    assert instance.call_cnt == 4
