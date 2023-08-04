"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from astacus.client import _reload_config
from freezegun import freeze_time
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
    sleep_times = []
    with freeze_time("2020-01-02") as current_time:

        def sleep(duration: float) -> None:
            sleep_times.append(duration)
            current_time.tick(delta=datetime.timedelta(seconds=duration))

        with mock.patch.object(time, "sleep", new=sleep):
            with mock.patch.object(astacus.client, "http_request") as http_request:
                http_request.return_value = None
                _reload_config(ClientArgs(wait_completion=10, url="http://localhost:55123"))
    assert sleep_times == [0.1, 0.2, 0.4, 0.8, 1.6, 3.2]
    assert http_request.call_count == 7
