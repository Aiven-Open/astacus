"""utils.

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Shared utilities (between coordinator and node)

"""

from __future__ import annotations

from abc import ABC
from collections import deque
from collections.abc import AsyncIterable, AsyncIterator, Callable, Hashable, Iterable, Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
from multiprocessing.dummy import Pool  # fastapi + fork = bad idea
from pathlib import Path
from pydantic import BaseModel
from typing import Any, Final, Generic, IO, Literal, overload, TextIO, TypeAlias, TypeVar

import asyncio
import contextlib
import datetime
import httpcore
import httpx
import json as _json
import logging
import os
import re
import requests
import tempfile
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
    """Get or create sub-state entry (using factory callback)."""
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
    logger.info("request %s %s by %s", method, url, caller)
    try:
        # There is a combination of bugs between urllib3<2 and uvicorn<0.24.0
        # where the abrupt socket shutdown of urllib3 causes the uvicorn server
        # to wait for a long time when shutting down, only on Python>=3.12.
        # Since we don't use multi-requests sessions, disable Keep-Alive to
        # allow the server to shut down the connection on its side as soon as
        # it is done replying.
        kw["headers"] = {"Connection": "close", **kw.get("headers", {})}
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
    url: str,
    *,
    caller: str,
    method: str = "get",
    timeout: float = 10.0,
    json: bool = True,
    ignore_status_code: bool = False,
    **kw,
) -> httpx.Response | Mapping[str, Any] | None:
    """Wrapper for httpx.request which handles timeouts as non-exceptions,
    and returns only valid results that we actually care about.
    """
    r = None
    # TBD: may need to redact url in future, if we actually wind up
    # using passwords in urls here.
    logger.info("async-request %s %s by %s", method, url, caller)
    # httpx has a silly bug where it (very slowly) loads the ssl context even when it's not used at all.
    # we can work around that by disabling certificates verification when the protocol is HTTP.
    # A better fix would be to have a proper long-living scope for the client, but this requires large
    # changes in astacus API implementation.
    verify = not url.startswith("http://")
    async with httpx.AsyncClient(verify=verify) as client:
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


@contextlib.asynccontextmanager
async def httpx_request_stream(
    url: str | httpx.URL,
    *,
    caller: str,
    method: str = "get",
    timeout: float = 10.0,
    ignore_status_code: bool = False,
    **kw,
) -> AsyncIterator[httpx.Response | None]:
    """Wrapper for httpx.request which handles timeouts as non-exceptions,
    and returns only valid results that we actually care about.
    """
    # TBD: may need to redact url in future, if we actually wind up
    # using passwords in urls here.
    logger.info("async-request %s %s by %s", method, url, caller)
    async with httpx.AsyncClient() as client:
        try:
            async with client.stream(method, url, timeout=timeout, **kw) as r:
                if not r.is_error or ignore_status_code:
                    yield r
                    return
                await r.aread()
                logger.warning("Unexpected response status code from %s to %s: %s %r", url, caller, r.status_code, r.text)
        except (httpcore.NetworkError, httpcore.TimeoutException) as ex:
            # Unfortunately at least current httpx leaks these
            # exceptions without wrapping them. Future versions may
            # address this hopefully. I believe httpx.TransportError
            # replaces it in future versions once we upgrade.
            logger.warning("Network error from %s to %s: %r", url, caller, ex)
        except httpx.HTTPError as ex:
            logger.warning("Unexpected response from %s to %s: %r", url, caller, ex)
        yield None


def build_netloc(host: str, port: int | None = None) -> str:
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
    retries: int | None = None,
    multiplier: float = 2,
    maximum: float | None = None,
    duration: float | None = None,
    async_sleeper: AsyncSleeper | None = None,
) -> AnyIterable[int]:
    """Exponential backoff iterator which works with both 'for' and 'async for'.

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
            logger.info("exponential_backoff waiting %.2f seconds (retry %d%s)", delay, self.retry, retries_text)
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
        self._f = open(path, "rb")  # noqa: SIM115
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


def size_as_short_str(size: int | float) -> str:
    for unit, multiplier in [("T", 1e12), ("G", 1e9), ("M", 1e6), ("K", 1e3)]:
        if size >= multiplier:
            unit_size = size / multiplier
            return f"{unit_size:.1f} {unit}B"
    return f"{size} B"


def parallel_map_to(*, fun, iterable, result_callback, n=None) -> bool:
    iterable_as_list = list(iterable)
    with Pool(n) as p:
        for map_in, map_out in zip(iterable_as_list, p.imap(fun, iterable_as_list)):
            if not result_callback(map_in=map_in, map_out=map_out):
                return False
    return True


def now():
    return datetime.datetime.now(datetime.UTC)


def monotonic_time():
    return time.monotonic()


Write: TypeAlias = Literal["w"]
UpdateBinary: TypeAlias = Literal["w+b"]


@overload
def open_path_with_atomic_rename(path: os.PathLike, *, mode: UpdateBinary) -> AbstractContextManager[IO[bytes]]: ...


@overload
def open_path_with_atomic_rename(path: os.PathLike, *, mode: Write) -> AbstractContextManager[TextIO]: ...


@overload
def open_path_with_atomic_rename(path: os.PathLike) -> AbstractContextManager[IO[bytes]]: ...


def open_path_with_atomic_rename(path: os.PathLike, *, mode: str = "w+b") -> AbstractContextManager[TextIO | IO[bytes]]:
    return _open_path_with_atomic_rename(path, mode=mode)


@contextmanager
def _open_path_with_atomic_rename(path: os.PathLike, *, mode: str) -> Iterator[TextIO | IO[bytes]]:
    """Pathlib-style ~atomic file replacement.

    This utility function creates new temporary file 'near' the path
    (=in the same directory, with .astacus.tmp suffix). If the file
    creation succeeds (=no exception is thrown), the temporary file is
    renamed to the target file.

    Note: IO[bytes] used instead of BinaryIO because the object returned by NamedTemporaryFile
    (_TemporaryFileWrapper[bytes]) isn't a BinaryIO. See https://github.com/python/typeshed/issues/7843
    """
    if not isinstance(path, Path):
        path = Path(path)
    with tempfile.NamedTemporaryFile(prefix=path.name, dir=path.parent, suffix=".astacus.tmp", delete=False, mode=mode) as f:
        try:
            yield f
            os.rename(f.name, path)
        except:
            os.unlink(f.name)
            raise


FALLBACK_UMASK: Final[int] = 0o022


def get_umask() -> int:
    try:
        proc_status = Path("/proc/self/status").read_text()
    except FileNotFoundError:
        return FALLBACK_UMASK
    return parse_umask(proc_status)


def parse_umask(proc_status: str) -> int:
    if umask_line := re.search(r"^Umask:\s+(0\d\d\d)$", proc_status, flags=re.MULTILINE):
        return int(umask_line.group(1), 8)
    return FALLBACK_UMASK


FifoValue = TypeVar("FifoValue")


class FifoCache(Generic[FifoValue]):
    def __init__(self, size: int = 1) -> None:
        if size < 1:
            raise ValueError("FifoCache size must be at least 1")
        self.size: int = size
        self.cache: dict[Hashable, FifoValue] = {}
        self.key_queue: deque[Hashable] = deque()

    def __contains__(self, key: Hashable) -> bool:
        return key in self.cache

    def __getitem__(self, key: Hashable) -> FifoValue | None:
        return self.cache.get(key)

    def __setitem__(self, key: Hashable, value: FifoValue) -> None:
        if key in self.cache:
            self.cache[key] = value
        if len(self.cache) >= self.size:
            self.cache.pop(self.key_queue.popleft())
        self.key_queue.append(key)
        self.cache[key] = value

    def __len__(self) -> int:
        return len(self.cache)


CacheMethodClassT = TypeVar("CacheMethodClassT", bound=object)
CacheFuncArg = TypeVar("CacheFuncArg", bound=Hashable)
CacheValue = TypeVar("CacheValue")
CachedMethod: TypeAlias = Callable[[CacheMethodClassT, CacheFuncArg], CacheValue]


def fifo_cache(size: int) -> Callable[[CachedMethod], CachedMethod]:
    """Decorator for caching method results in a FIFO cache of given size.

    The decorated method must take only one hashable argument.
    The cache is per-instance.
    Not thread-safe.
    """

    def decorator(method: CachedMethod) -> Callable:
        cache_attribute_name = "__fifo_cache_" + method.__name__

        # Type alias cannot be used here, as mypy cannot deduct from inner types
        def wrapper(self: Callable[[CacheMethodClassT, CacheFuncArg], CacheValue], key: CacheFuncArg) -> CacheValue:
            cache: FifoCache[CacheValue] | None = getattr(self, cache_attribute_name, None)
            if cache is None:
                cache = FifoCache[CacheValue](size)
                setattr(self, cache_attribute_name, cache)
            if key in cache:
                val = cache[key]
                assert val is not None
                return val
            value = method(self, key)
            cache[key] = value
            return value

        return wrapper

    return decorator
