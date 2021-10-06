"""

utils

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Shared utilities (between coordinator and node)

"""
from abc import ABC
from multiprocessing.dummy import Pool  # fastapi + fork = bad idea
from pydantic import BaseModel
from typing import Any, AsyncIterable, Callable, Dict, Iterable, Optional, TypeVar, Union

import asyncio
import datetime
import httpcore
import httpx
import json as _json
import logging
import os
import requests
import time

logger = logging.getLogger(__name__)


class AstacusModel(BaseModel):
    class Config:
        # As we're keen to both export and decode json, just using
        # enum values for encode/decode is much saner than the default
        # enumname.value (it is also slightly less safe but oh well)
        use_enum_values = True

        # Extra values should be errors, as they are most likely typos
        # which lead to grief when not detected. However, if we ever
        # start deprecating some old fields and not wanting to parse
        # them, this might need to be revisited.
        extra = "forbid"

        # Validate field default values too
        validate_all = True

        # Validate also assignments
        # validate_assignment = True
        # TBD: Figure out why this doesn't work in some unit tests;
        # possibly the tests themselves are broken

    def jsondict(self, **kw):
        # By default,
        #
        # .json() returns json string.
        # .dict() returns Python dict, but it has things that are not
        # json serializable.
        #
        # We provide json seralizable dict (super inefficiently) here.
        #
        # This is mostly used for test code so that should be fine
        return _json.loads(self.json(**kw))


def get_or_create_state(*, state: object, key: str, factory: Callable[[], Any]) -> Any:
    """ Get or create sub-state entry (using factory callback) """
    value = getattr(state, key, None)
    if value is None:
        value = factory()
        setattr(state, key, value)
    return value


def http_request(url, *, caller, method="get", timeout=10, ignore_status_code: bool = False, **kw):
    """Wrapper for requests.request which handles timeouts as non-exceptions,
    and returns only valid results that we actually care about.

    This is here primarily so that some requests stuff
    (e.g. fastapi.testclient) still works, but we can mock things to
    our hearts content in test code by doing 'things' here.
    """
    # TBD: may need to redact url in future, if we actually wind up
    # using passwords in urls here.
    logger.debug("request %s %s by %s", method, url, caller)
    try:
        r = requests.request(method, url, timeout=timeout, **kw)
        if r.ok:
            return r.json()
        if ignore_status_code:
            return r.json()
        logger.warning("Unexpected response from %s to %s: %s %r", url, caller, r.status_code, r.text)
    except requests.RequestException as ex:
        logger.warning("Unexpected response from %s to %s: %r", url, caller, ex)
    return None


async def httpx_request(
    url: Union[str, httpx.URL],
    *,
    caller: str,
    method: str = "get",
    timeout: float = 10.0,
    json: bool = True,
    ignore_status_code: bool = False,
    **kw,
) -> Optional[Union[httpx.Response, Dict]]:
    """Wrapper for httpx.request which handles timeouts as non-exceptions,
    and returns only valid results that we actually care about.
    """
    r = None
    # TBD: may need to redact url in future, if we actually wind up
    # using passwords in urls here.
    logger.debug("async-request %s %s by %s", method, url, caller)
    async with httpx.AsyncClient() as client:
        try:
            r = await client.request(method, url, timeout=timeout, **kw)
            if not r.is_error:
                return r.json() if json else r
            if ignore_status_code:
                return r.json() if json else r
            logger.warning("Unexpected response status code from %s to %s: %s %r", url, caller, r.status_code, r.text)
        except (httpcore.NetworkError, httpcore.TimeoutException) as ex:
            # Unfortunately at least current httpx leaks these
            # exceptions without wrapping them. Future versions may
            # address this hopefully. I believe httpx.TransportError
            # replaces it in future versions once we upgrade.
            logger.warning("Network error from %s to %s: %r", url, caller, ex)
        except httpx.HTTPError as ex:
            logger.warning("Unexpected response from %s to %s: %r", url, caller, ex)
        return None


def build_netloc(host: str, port: Optional[int] = None) -> str:
    """Create a netloc that can be passed to `url.parse.urlunsplit` while safely handling ipv6 addresses."""
    escaped_host = f"[{host}]" if ":" in host else host
    return escaped_host if port is None else f"{escaped_host}:{port}"


T = TypeVar("T")


class AnyIterable(Iterable[T], AsyncIterable[T], ABC):
    pass


class AsyncSleeper:
    def __init__(self):
        self.wakeup_event = asyncio.Event()

    def wakeup(self) -> None:
        """Wake up another coroutine waiting using `sleep`."""
        self.wakeup_event.set()

    async def sleep(self, seconds: float) -> None:
        """Wait for `seconds` unless interrupted by `wakeup`."""
        coros = [asyncio.sleep(seconds), self.wakeup_event.wait()]
        tasks = [asyncio.create_task(coro) for coro in coros]
        _, pending_tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for pending_task in pending_tasks:
            pending_task.cancel()
        self.wakeup_event.clear()


def exponential_backoff(
    *,
    initial: float,
    retries: Optional[int] = None,
    multiplier: float = 2,
    maximum: Optional[float] = None,
    duration: Optional[float] = None,
    async_sleeper: Optional[AsyncSleeper] = None,
) -> AnyIterable[int]:
    """Exponential backoff iterator which works with both 'for' and 'async for'

    First attempt is never delayed. The delays are only for retries.
    'initial' is the first retry's delay. After that, each retry is
    multiplier times larger.

    The iteration stops if:
    - retries is exceeded (retries=0 = try only once, although not very useful)
    - duration would be exceeded (if duration is provided)
    """
    retries_text = ""
    if retries:
        retries_text = f"/{retries}"

    class _Iter:
        retry = -1
        initial = None

        @property
        def _delay(self):
            if retries is not None and self.retry > retries:
                return None
            time_now = time.monotonic()
            if not self.retry:
                self.initial = time_now
                return 0
            delay = initial * multiplier ** (self.retry - 1)
            if maximum is not None:
                delay = min(delay, maximum)
            if duration is not None:
                time_left_after_sleep = (self.initial + duration) - time_now - delay
                if time_left_after_sleep < 0:
                    return None
            logger.debug("exponential_backoff waiting %.2f seconds (retry %d%s)", delay, self.retry, retries_text)

            return delay

        async def __anext__(self):
            self.retry += 1
            delay = self._delay
            if delay is None:
                raise StopAsyncIteration
            if delay:
                nonlocal async_sleeper
                if async_sleeper is None:
                    async_sleeper = AsyncSleeper()
                await async_sleeper.sleep(delay)
            return self.retry

        def __next__(self):
            assert async_sleeper is None
            self.retry += 1
            delay = self._delay
            if delay is None:
                raise StopIteration
            if delay:
                time.sleep(delay)
            return self.retry

    class _Iterable(AnyIterable[int]):
        def __aiter__(self):
            return _Iter()

        def __iter__(self):
            return _Iter()

    assert duration is not None or retries is not None
    return _Iterable()


class SizeLimitedFile:
    def __init__(self, *, path, file_size):
        self._f = open(path, "rb")  # pylint: disable=consider-using-with
        self._file_size = file_size
        self.tell = self._f.tell

    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        self._f.close()

    def read(self, n=None):
        can_read = max(0, self._file_size - self._f.tell())
        if n is None:
            n = can_read
        n = min(can_read, n)
        return self._f.read(n)

    def seek(self, ofs, whence=0):
        if whence == os.SEEK_END:
            ofs += self._file_size
            whence = os.SEEK_SET
        if whence == os.SEEK_SET:
            ofs = max(0, min(self._file_size, ofs))
        return self._f.seek(ofs, whence)


def timedelta_as_short_str(delta):
    h, s = divmod(delta.seconds, 3600)
    m, s = divmod(s, 60)
    return " ".join(f"{v}{u}" for v, u in [(delta.days, "d"), (h, "h"), (m, "m"), (s, "s")] if v)


def size_as_short_str(s):
    for u, m in [("T", 1e12), ("G", 1e9), ("M", 1e6), ("K", 1e3)]:
        if s >= m:
            return "%.1f %sB" % (s / m, u)
    return f"{s} B"


def parallel_map_to(*, fun, iterable, result_callback, n=None) -> bool:
    iterable_as_list = list(iterable)
    with Pool(n) as p:
        for map_in, map_out in zip(iterable_as_list, p.imap(fun, iterable_as_list)):
            if not result_callback(map_in=map_in, map_out=map_out):
                return False
    return True


def now():
    return datetime.datetime.now(datetime.timezone.utc)


def monotonic_time():
    return time.monotonic()
