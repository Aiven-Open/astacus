"""

utils

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Shared utilities (between coordinator and node)

"""

from starlette.requests import Request

import httpx
import logging
import requests

logger = logging.getLogger(__name__)


def get_or_create_state(*, request: Request, key: str, factory):
    """ Get or create sub-state entry (using factory callback) """
    value = getattr(request.app.state, key, None)
    if value is None:
        value = factory()
        setattr(request.app.state, key, value)
    return value


def http_request(url,
                 *,
                 caller,
                 method="get",
                 timeout=10,
                 ignore_status_code: bool = False):
    """Wrapper for requests.request which handles timeouts as non-exceptions,
    and returns only valid results that we actually care about.

    This is here primarily so that some requests stuff
    (e.g. fastapi.testclient) still works, but we can mock things to
    our hearts content in test code by doing 'things' here.
    """
    r = None
    # TBD: may need to redact url in future, if we actually wind up
    # using passwords in urls here.
    logger.info("request %s %s by %s", method, url, caller)
    try:
        r = requests.request(method, url, timeout=timeout)
        if r.ok:
            return r.json()
        if ignore_status_code:
            return r
        logger.warning("Unexpected response from %s to %s: %s %r", url, caller,
                       r.status_code, r.text)
    except requests.RequestException as ex:
        logger.warning("Unexpected response from %s to %s: %r", url, caller,
                       ex)
    return None


async def httpx_request(url,
                        *,
                        caller,
                        method="get",
                        timeout=10,
                        ignore_status_code: bool = False):
    """Wrapper for httpx.request which handles timeouts as non-exceptions,
    and returns only valid results that we actually care about.
    """
    r = None
    # TBD: may need to redact url in future, if we actually wind up
    # using passwords in urls here.
    logger.info("request %s %s by %s", method, url, caller)
    async with httpx.AsyncClient() as client:
        try:
            r = await client.request(method, url, timeout=timeout)
            if not r.is_error:
                return r.json()
            if ignore_status_code:
                return r
            logger.warning("Unexpected response from %s to %s: %s %r", url,
                           caller, r.status_code, r.text)
        except httpx.HTTPError as ex:
            logger.warning("Unexpected response from %s to %s: %r", url,
                           caller, ex)
        return None
