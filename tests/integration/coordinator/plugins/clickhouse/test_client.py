"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from .conftest import ClickHouseCommand, create_clickhouse_service, get_clickhouse_client
from astacus.coordinator.plugins.clickhouse.client import ClickHouseClientQueryError
from tests.integration.conftest import Ports, Service
from typing import cast, Sequence

import pytest
import time

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.order("second_to_last"),
]


@pytest.mark.asyncio
async def test_client_execute(clickhouse: Service) -> None:
    client = get_clickhouse_client(clickhouse)
    response = cast(Sequence[list[str]], await client.execute(b"SHOW DATABASES"))
    assert sorted(list(response)) == [["INFORMATION_SCHEMA"], ["default"], ["information_schema"], ["system"]]


@pytest.mark.asyncio
async def test_client_execute_on_system_database(clickhouse: Service) -> None:
    client = get_clickhouse_client(clickhouse)
    response = await client.execute(b"SELECT currentDatabase()")
    assert response == [["system"]]


@pytest.mark.asyncio
async def test_client_execute_with_empty_response(clickhouse: Service) -> None:
    # In that case, ClickHouse http protocol doesn't bother with replying with
    # an empty json dict and instead replies with an empty string.
    client = get_clickhouse_client(clickhouse)
    # This query should be harmless enough
    response = await client.execute(b"SYSTEM DROP DNS CACHE")
    assert response == []


@pytest.mark.asyncio
async def test_client_execute_bounded_connection_failure_time(ports: Ports, clickhouse_command: ClickHouseCommand) -> None:
    async with create_clickhouse_service(ports, clickhouse_command) as clickhouse:
        client = get_clickhouse_client(clickhouse, timeout=1.0)
        clickhouse.process.kill()
        start_time = time.monotonic()
        with pytest.raises(ClickHouseClientQueryError):
            await client.execute(b"SHOW DATABASES")
        elapsed_time = time.monotonic() - start_time
        assert elapsed_time < 10.0


@pytest.mark.asyncio
async def test_client_execute_bounded_query_time(clickhouse: Service) -> None:
    client = get_clickhouse_client(clickhouse, timeout=1.0)
    start_time = time.monotonic()
    with pytest.raises(ClickHouseClientQueryError):
        await client.execute(b"SELECT 1,sleepEachRow(3)")
    elapsed_time = time.monotonic() - start_time
    assert 1.0 <= elapsed_time < 3.0


@pytest.mark.asyncio
async def test_client_execute_timeout_can_be_customized_per_query(clickhouse: Service) -> None:
    client = get_clickhouse_client(clickhouse, timeout=10.0)
    start_time = time.monotonic()
    # The maximum sleep time in ClickHouse is 3 seconds
    with pytest.raises(ClickHouseClientQueryError):
        await client.execute(b"SELECT 1,sleepEachRow(3)", timeout=1)
    elapsed_time = time.monotonic() - start_time
    assert 1.0 <= elapsed_time < 3.0


@pytest.mark.asyncio
async def test_client_execute_error_returns_status_and_exception_code(clickhouse: Service) -> None:
    unknown_table_exception_code = 60
    client = get_clickhouse_client(clickhouse)
    with pytest.raises(ClickHouseClientQueryError) as raised:
        await client.execute(b"SELECT * FROM default.does_not_exist", timeout=1)
    assert raised.value.status_code == 404
    assert raised.value.exception_code == unknown_table_exception_code
