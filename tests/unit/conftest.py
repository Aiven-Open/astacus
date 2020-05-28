"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from astacus.common import utils

import logging
import pytest

logger = logging.getLogger(__name__)


@pytest.fixture(name="utils_http_request_list")
def fixture_utils_http_request_list(mocker):
    result_list = []

    def _http_request(*args, **kwargs):
        logger.info("utils_http_request_list %r %r", args, kwargs)
        assert result_list, f"Unable to serve request {args!r} {kwargs!r}"
        r = result_list.pop(0)
        if isinstance(r, dict):
            if "args" in r:
                assert list(r["args"]) == list(args)
            if "kwargs" in r:
                assert r["kwargs"] == kwargs
            if "result" in r:
                return r["result"]
            return None
        if r is None:
            return None
        raise NotImplementedError(f"Unknown item in utils_request_list: {r!r}")

    mocker.patch.object(utils, "http_request", new=_http_request)
    yield result_list
    assert not result_list, f"Unconsumed requests: {result_list!r}"
