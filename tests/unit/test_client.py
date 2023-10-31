"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from astacus.client import _reload_config
from freezegun import freeze_time
from typing import Any, Mapping
from unittest import mock

import astacus.client
import dataclasses
import datetime
import time


@dataclasses.dataclass(frozen=True)
class ClientArgs:
    wait_completion: float
    url: str


def test_reload_config_retries_on_connection_failure() -> None:
    with freeze_time("2020-01-02") as current_time:

        def sleep(duration: float) -> None:
            current_time.tick(delta=datetime.timedelta(seconds=duration))

        with mock.patch.object(time, "sleep", new=sleep):
            with mock.patch.object(astacus.client, "http_request") as http_request:
                http_request.return_value = None
                outcome = _reload_config(ClientArgs(wait_completion=10, url="http://localhost:55123"))
    assert http_request.call_count > 1
    assert outcome is False


def test_reload_config_sleep_and_http_timeout_honors_total_wait_completion() -> None:
    wait_completion_secs = 10.0
    with freeze_time("2020-01-02") as current_time:
        start_time = time.monotonic()

        def sleep(duration: float) -> None:
            time_since_start = time.monotonic() - start_time
            assert time_since_start + duration <= wait_completion_secs, "sleep will end after completion"
            current_time.tick(delta=datetime.timedelta(seconds=duration))

        with mock.patch.object(time, "sleep", new=sleep):
            # mypy wants a return for this function but pylint doesn't
            # pylint: disable=useless-return
            def http_request(*args, timeout: float = 10, **kwargs) -> Mapping[str, Any] | None:
                time_since_start = time.monotonic() - start_time
                assert time_since_start + timeout <= wait_completion_secs, "request could end after completion"
                return None

            with mock.patch.object(astacus.client, "http_request", new=http_request):
                _reload_config(ClientArgs(wait_completion=wait_completion_secs, url="http://localhost:55123"))


def test_reload_config_stops_retrying_on_success() -> None:
    return_values: list[Mapping[str, Any] | None] = [None, None, {}]
    with mock.patch.object(time, "sleep"):

        def http_request(*args, **kwargs) -> Mapping[str, Any] | None:
            assert len(return_values) > 0, "http_request called too many times"
            return return_values.pop(0)

        with mock.patch.object(astacus.client, "http_request", new=http_request):
            outcome = _reload_config(ClientArgs(wait_completion=10, url="http://localhost:55123"))
    assert return_values == []
    assert outcome is True
