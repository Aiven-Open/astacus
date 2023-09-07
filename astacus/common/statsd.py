"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

StatsD client

Supports telegraf's statsd protocol extension for 'key=value' tags:

  https://github.com/influxdata/telegraf/tree/master/plugins/inputs/statsd

This is combination of:
- pghoard base (pghoard.metrics.statsd)
- myhoard timing_manager method
- pydantic configuration + async_timing_manager + explicit typing

"""
from .magic import StrEnum
from .utils import AstacusModel
from contextlib import contextmanager
from typing import Optional, Union

import socket
import time


class MessageFormat(StrEnum):
    datadog = "datadog"
    telegraf = "telegraf"


Tags = dict[str, Union[int, str, None]]


class StatsdConfig(AstacusModel):
    host: str = "127.0.0.1"
    port: int = 8125
    message_format: MessageFormat = MessageFormat.telegraf
    tags: Tags = {}


class StatsClient:
    _enabled = True

    def __init__(self, config: Optional[StatsdConfig]):
        if not config:
            self._enabled = False
            return
        self._dest_addr = (config.host, config.port)
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._tags = config.tags
        self._message_format = config.message_format

    @contextmanager
    def timing_manager(self, metric: str, tags: Optional[Tags] = None):
        start_time = time.monotonic()
        tags = (tags or {}).copy()
        try:
            yield
        except:  # noqa pylint: disable=broad-except,bare-except
            tags["success"] = "0"
            self.timing(metric, time.monotonic() - start_time, tags=tags)
            raise
        tags["success"] = "1"
        self.timing(metric, time.monotonic() - start_time, tags=tags)

    def gauge(self, metric: str, value: Union[int, float], *, tags: Optional[Tags] = None) -> None:
        self._send(metric, b"g", value, tags)

    def increase(self, metric: str, *, inc_value: int = 1, tags: Optional[Tags] = None) -> None:
        self._send(metric, b"c", inc_value, tags)

    def timing(self, metric: str, value: Union[int, float], *, tags: Optional[Tags] = None) -> None:
        self._send(metric, b"ms", value, tags)

    def unexpected_exception(self, ex, where, *, tags: Optional[Tags] = None) -> None:
        all_tags = {
            "exception": ex.__class__.__name__,
            "where": where,
        }
        all_tags.update(tags or {})
        self.increase("exception", tags=all_tags)

    def _send(self, metric: str, metric_type: bytes, value: Union[int, float], tags: Optional[Tags]) -> None:
        if not self._enabled:
            # stats sending is disabled
            return

        # telegraf format: "user.logins,service=payroll,region=us-west:1|c"
        # datadog format: metric.name:value|type|@sample_rate|#tag1:value,tag2
        #                 http://docs.datadoghq.com/guides/dogstatsd/#datagram-format

        parts = [metric.encode("utf-8"), b":", str(value).encode("utf-8"), b"|", metric_type]
        send_tags = self._tags.copy()
        send_tags.update(tags or {})
        if self._message_format == MessageFormat.datadog:
            for index, (tag, val) in enumerate(send_tags.items()):
                if index == 0:
                    separator = "|#"
                else:
                    separator = ","
                if val is None:
                    pattern = "{}{}"
                else:
                    pattern = "{}{}:{}"
                parts.append(pattern.format(separator, tag, val).encode("utf-8"))
        elif self._message_format == MessageFormat.telegraf:
            for tag, val in send_tags.items():
                parts.insert(1, f",{tag}={val}".encode("utf-8"))
        else:
            raise NotImplementedError("Unsupported message format")

        self._socket.sendto(b"".join(parts), self._dest_addr)
