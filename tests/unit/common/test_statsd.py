"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Test that StatsClient works as advertised.

This could be broader, but at least minimal functionality is tested here.

"""

from astacus.common import statsd

import asyncio
import pytest
import socket


@pytest.mark.asyncio
async def test_statsd():
    loop = asyncio.get_running_loop()
    received = asyncio.Queue()

    class _Protocol:
        def connection_made(self, x):
            pass

        def datagram_received(self, data, _):
            received.put_nowait(data)

    def _protocol_factory():
        return _Protocol()

    transport, _ = await loop.create_datagram_endpoint(_protocol_factory, family=socket.AF_INET)
    sock = transport.get_extra_info('socket')
    sock.bind(('', 0))
    port = sock.getsockname()[-1]

    # Ensure that no config = no action
    c = statsd.StatsClient(config=None)
    c.increase("foo")

    c = statsd.StatsClient(config=statsd.StatsdConfig(port=port))
    c.increase("bar")

    data = await received.get()
    assert data == b'bar:1|c'

    with c.timing_manager("sync_timing"):
        pass
    data = await received.get()
    assert data.startswith(b'sync_timing,success=1:') and data.endswith(b'|ms')

    async with c.async_timing_manager("async_timing"):
        pass
    data = await received.get()
    assert data.startswith(b'async_timing,success=1:') and data.endswith(b'|ms')
