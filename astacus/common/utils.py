"""

utils

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Shared utilities (between coordinator and node)

"""

from pydantic import BaseModel
from starlette.requests import Request

import asyncio
import httpx
import logging
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


def get_or_create_state(*, request: Request, key: str, factory):
    """ Get or create sub-state entry (using factory callback) """
    value = getattr(request.app.state, key, None)
    if value is None:
        value = factory()
        setattr(request.app.state, key, value)
    return value


def http_request(url, *, caller, method="get", timeout=10, ignore_status_code: bool = False, **kw):
    """Wrapper for requests.request which handles timeouts as non-exceptions,
    and returns only valid results that we actually care about.

    This is here primarily so that some requests stuff
    (e.g. fastapi.testclient) still works, but we can mock things to
    our hearts content in test code by doing 'things' here.
    """
    r = None
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


async def httpx_request(url, *, caller, method="get", timeout=10, json: bool = True, ignore_status_code: bool = False, **kw):
    """Wrapper for httpx.request which handles timeouts as non-exceptions,
    and returns only valid results that we actually care about.
    """
    r = None
    # TBD: may need to redact url in future, if we actually wind up
    # using passwords in urls here.
    logger.debug("request %s %s by %s", method, url, caller)
    async with httpx.AsyncClient() as client:
        try:
            r = await client.request(method, url, timeout=timeout, **kw)
            if not r.is_error:
                return r.json() if json else r
            if ignore_status_code:
                return r.json() if json else r
            logger.warning("Unexpected response from %s to %s: %s %r", url, caller, r.status_code, r.text)
        except httpx.HTTPError as ex:
            logger.warning("Unexpected response from %s to %s: %r", url, caller, ex)
        except AssertionError as ex:
            logger.error("AssertionError - probably respx - from %s to %s: %r", url, caller, ex)
        return None


def exponential_backoff(*, initial, retries=None, multiplier=2, maximum=None, duration=None):
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
            now = time.monotonic()
            if not self.retry:
                self.initial = now
                return 0
            delay = initial * multiplier ** (self.retry - 1)
            if maximum is not None:
                delay = min(delay, maximum)
            if duration is not None:
                time_left_after_sleep = (self.initial + duration) - now - delay
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
                await asyncio.sleep(delay)
            return self.retry

        def __next__(self):
            self.retry += 1
            delay = self._delay
            if delay is None:
                raise StopIteration
            if delay:
                time.sleep(delay)
            return self.retry

    class _Iterable:
        def __aiter__(self):
            return _Iter()

        def __iter__(self):
            return _Iter()

    assert duration is not None or retries is not None
    return _Iterable()


class SizeLimitedFile:
    def __init__(self, *, path, file_size):
        self.f = open(path, "rb")
        self.file_size = file_size
        self.ofs = 0

    def read(self, n=None):
        can_read = max(0, self.file_size - self.ofs)
        if n is None:
            n = can_read
        n = min(can_read, n)
        data = self.f.read(n)
        self.ofs += len(data)
        return data

    def seek(self, ofs, whence=0):
        if whence == 0:  # from start of file
            pass
        elif whence == 2:  # from end of file
            ofs += self.file_size
        else:
            raise NotImplementedError
        if ofs < 0:
            raise ValueError(f"negative seek position: {ofs}")
        self.ofs = ofs
        return ofs

    def tell(self):
        return self.ofs


def timedelta_as_short_str(delta):
    h, s = divmod(delta.seconds, 3600)
    m, s = divmod(s, 60)
    return " ".join(f"{v}{u}" for v, u in [(delta.days, "d"), (h, "h"), (m, "m"), (s, "s")] if v)


def size_as_short_str(s):
    for u, m in [("T", 1e12), ("G", 1e9), ("M", 1e6), ("K", 1e3)]:
        if s >= m:
            return "%.1f %sB" % (s / m, u)
    return f"{s} B"
