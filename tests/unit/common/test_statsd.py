"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Test that StatsClient works as advertised.

This could be broader, but at least minimal functionality is tested here.

"""
from __future__ import annotations

from astacus.common import statsd
from typing import Any

import asyncio
import pytest
import socket


class _Protocol(asyncio.DatagramProtocol):
    def __init__(self, queue: asyncio.Queue):
        self.received_queue = queue

    def datagram_received(self, data: bytes, addr: tuple[str | Any, int]) -> None:
        self.received_queue.put_nowait(data)


@pytest.mark.asyncio
async def test_statsd():
    loop = asyncio.get_running_loop()
    received = asyncio.Queue()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("", 0))

    transport, _ = await loop.create_datagram_endpoint(lambda: _Protocol(received), sock=sock)
    sock = transport.get_extra_info("socket")
    port = sock.getsockname()[-1]

    # Ensure that no config = no action
    c = statsd.StatsClient(config=None)
    c.increase("foo")

    c = statsd.StatsClient(config=statsd.StatsdConfig(port=port))
    c.increase("bar")

    data = await received.get()
    assert data == b"bar:1|c"

    with c.timing_manager("sync_timing"):
        pass
    data = await received.get()
    assert data.startswith(b"sync_timing,success=1:") and data.endswith(b"|ms")

    async with c.async_timing_manager("async_timing"):
        pass
    data = await received.get()
    assert data.startswith(b"async_timing,success=1:") and data.endswith(b"|ms")
