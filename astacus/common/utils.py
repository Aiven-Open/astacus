"""

utils

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Shared utilities (between coordinator and node)

"""

from pydantic import BaseModel
from starlette.requests import Request

import httpx
import logging
import requests

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
