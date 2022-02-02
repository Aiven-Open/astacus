"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from astacus.coordinator.plugins.clickhouse.client import (
    ClickHouseClientQueryError, escape_sql_identifier, HttpClickHouseClient
)

import json
import pytest
import respx

pytestmark = [pytest.mark.clickhouse]


@pytest.mark.asyncio
async def test_successful_execute_returns_rows() -> None:
    client = HttpClickHouseClient(host="example.org", port=9000)
    with respx.mock:
        # Source: https://clickhouse.tech/docs/en/interfaces/formats/#jsoncompact
        content = {
            "meta": [{
                "name": "name",
                "type": "String"
            }],
            "data": [["system"], ["defaultdb"]],
            "rows": 2,
            "rows_before_limit_at_least": 2
        }
        respx.post("http://example.org:9000?query=SHOW+DATABASES&wait_end_of_query=1").respond(json=content)
        response = await client.execute("SHOW DATABASES")
    assert response == [["system"], ["defaultdb"]]


@pytest.mark.asyncio
async def test_failing_execute_raises_an_exception() -> None:
    client = HttpClickHouseClient(host="example.org", port=9000)
    with respx.mock:
        respx.post("http://example.org:9000?query=SHOW+DATABASES&wait_end_of_query=1").respond(status_code=400)
        with pytest.raises(ClickHouseClientQueryError):
            await client.execute("SHOW DATABASES")


@pytest.mark.asyncio
async def test_sends_authentication_headers() -> None:
    client = HttpClickHouseClient(host="example.org", port=9000, username="user", password="password")
    with respx.mock:
        respx.post("http://example.org:9000?query=SHOW+DATABASES&wait_end_of_query=1").respond(content="")
        await client.execute("SELECT 1 LIMIT 0")
        request = respx.calls[0][0]
        assert request.headers["x-clickhouse-user"] == "user"
        assert request.headers["x-clickhouse-key"] == "password"


def test_escape_identifier() -> None:
    assert escape_sql_identifier("foo") == "`foo`"
    assert escape_sql_identifier("fo`o") == "`fo\\`o`"
    assert escape_sql_identifier("fo\\o") == "`fo\\\\o`"
