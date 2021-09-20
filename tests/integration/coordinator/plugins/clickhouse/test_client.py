"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from .conftest import get_clickhouse_client, Service

import pytest
import time

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.order("second_to_last"),
]


@pytest.mark.asyncio
async def test_client_execute(clickhouse: Service):
    client = get_clickhouse_client(clickhouse)
    response = await client.execute("SHOW DATABASES")
    assert response == [["default"], ["system"]]


@pytest.mark.asyncio
async def test_client_execute_on_system_database(clickhouse: Service):
    client = get_clickhouse_client(clickhouse)
    response = await client.execute("SELECT currentDatabase()")
    assert response == [["system"]]


@pytest.mark.asyncio
async def test_client_execute_with_empty_response(clickhouse: Service):
    # In that case, ClickHouse http protocol doesn't bother with replying with
    # an empty json dict and instead replies with an empty string.
    client = get_clickhouse_client(clickhouse)
    # This query should be harmless enough
    response = await client.execute("SYSTEM DROP DNS CACHE")
    assert response == []


@pytest.mark.asyncio
async def test_client_execute_bounded_failure_time(clickhouse: Service):
    client = get_clickhouse_client(clickhouse, timeout=1.0)
    client.timeout = 1
    clickhouse.process.kill()
    start_time = time.monotonic()
    with pytest.raises(Exception):
        ret = await client.execute("SHOW DATABASES")
        print(ret)
    elapsed_time = time.monotonic() - start_time
    assert elapsed_time < 10.0
