"""Copyright (c) 2024 Aiven Ltd
See LICENSE for details.
"""

from astacus.common.access_log import AccessLogLevelFilter
from collections.abc import Mapping
from typing import Any

import logging
import pytest


def create_record(msg: str, args: tuple[Any, ...] | Mapping[str, Any]) -> logging.LogRecord:
    return logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="/",
        lineno=0,
        msg=msg,
        args=args,
        exc_info=None,
    )


def create_access_record(method: str) -> logging.LogRecord:
    return create_record(
        msg='%s - "%s %s HTTP/%s" %d',
        args=("127.0.0.1:1234", method, "/foo", "HTTP/1.1"),
    )


def test_filter_lowers_level_of_get_method() -> None:
    record = create_access_record(method="GET")
    should_keep = AccessLogLevelFilter().filter(record)
    assert should_keep
    assert record.levelno == logging.DEBUG
    assert record.levelname == "DEBUG"


@pytest.mark.parametrize("method", ["POST", "PUT", "DELETE", "UNKNOWN"])
def test_filter_keeps_level_of_other_methods(method: str) -> None:
    record = create_access_record(method=method)
    should_keep = AccessLogLevelFilter().filter(record)
    assert should_keep
    assert record.levelno == logging.INFO
    assert record.levelname == "INFO"


@pytest.mark.parametrize(
    "record",
    [
        create_record(msg="no args", args=()),
        create_record(msg="one arg:%s", args=("arg",)),
        create_record(msg="arg1:%s, arg2: %s", args=("arg1", 2)),
        create_record(msg="dict arg: %(a)s", args=({"a": 1},)),
    ],
    ids=["no args", "one arg", "non-method arg2", "dict args"],
)
def test_filter_ignores_surprising_log_messages(record: logging.LogRecord) -> None:
    should_keep = AccessLogLevelFilter().filter(record)
    assert should_keep
    assert record.levelno == logging.INFO
    assert record.levelname == "INFO"
