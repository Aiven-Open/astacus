"""Copyright (c) 2021 Aiven Ltd
See LICENSE for details.
"""

from astacus.coordinator.plugins.clickhouse.client import (
    ClickHouseClientQueryError,
    escape_sql_identifier,
    HttpClickHouseClient,
    unescape_sql_string,
)

import pytest
import respx

pytestmark = [pytest.mark.clickhouse]


async def test_successful_execute_returns_rows() -> None:
    client = HttpClickHouseClient(host="example.org", port=9000)
    with respx.mock:
        # Source: https://clickhouse.tech/docs/en/interfaces/formats/#jsoncompact
        content = {
            "meta": [{"name": "name", "type": "String"}],
            "data": [["system"], ["defaultdb"]],
            "rows": 2,
            "rows_before_limit_at_least": 2,
        }
        respx.post("http://example.org:9000?wait_end_of_query=1").respond(json=content)
        response = await client.execute(b"SHOW DATABASES")
    assert response == [["system"], ["defaultdb"]]


async def test_failing_execute_raises_an_exception() -> None:
    client = HttpClickHouseClient(host="example.org", port=9000)
    with respx.mock:
        respx.post("http://example.org:9000?wait_end_of_query=1").respond(status_code=400)
        with pytest.raises(ClickHouseClientQueryError):
            await client.execute(b"SHOW DATABASES")


async def test_sends_authentication_headers() -> None:
    client = HttpClickHouseClient(host="example.org", port=9000, username="user", password="password")
    with respx.mock:
        respx.post("http://example.org:9000?wait_end_of_query=1").respond(content="")
        await client.execute(b"SELECT 1 LIMIT 0")
        request = respx.calls[0][0]
        assert request.headers["x-clickhouse-user"] == "user"
        assert request.headers["x-clickhouse-key"] == "password"


async def test_sends_session_id_as_parameter() -> None:
    client = HttpClickHouseClient(host="example.org", port=9000)
    with respx.mock:
        respx.post("http://example.org:9000?wait_end_of_query=1&session_id=something").respond(content="")
        await client.execute(b"SELECT 1 LIMIT 0", session_id="something")


def test_escape_sql_identifier() -> None:
    assert escape_sql_identifier(b"foo") == "`foo`"
    assert escape_sql_identifier(b"fo\\o") == "`fo\\\\o`"
    assert escape_sql_identifier(b"fo`o") == "`fo\\`o`"


def test_escape_sql_identifier_named_escaped() -> None:
    assert escape_sql_identifier(b"fo\bo") == "`fo\\bo`"
    assert escape_sql_identifier(b"fo\fo") == "`fo\\fo`"
    assert escape_sql_identifier(b"fo\ro") == "`fo\\ro`"
    assert escape_sql_identifier(b"fo\no") == "`fo\\no`"
    assert escape_sql_identifier(b"fo\to") == "`fo\\to`"
    assert escape_sql_identifier(b"fo\0o") == "`fo\\0o`"


def test_escape_sql_identifier_high_bytes() -> None:
    assert escape_sql_identifier(bytes((0x7E,))) == "`~`"
    for high_byte in range(0x7F, 0x100):
        assert escape_sql_identifier(bytes((high_byte,))) == f"`\\x{high_byte:02x}`"


def test_escape_sql_identifier_utf8() -> None:
    assert escape_sql_identifier("éléphant".encode()) == "`\\xc3\\xa9l\\xc3\\xa9phant`"


def test_unescape_sql_string() -> None:
    assert unescape_sql_string(b"''") == b""
    assert unescape_sql_string(b"'foo'") == b"foo"
    assert unescape_sql_string(b"'fo\\o'") == b"foo"
    assert unescape_sql_string(b"'foo\\\\bar'") == b"foo\\bar"
    assert unescape_sql_string(b"'foo\\x00bar'") == b"foo\x00bar"
    assert unescape_sql_string(b"'foo\\x80bar'") == b"foo\x80bar"
    assert unescape_sql_string(b"'foo\x80bar'") == b"foo\x80bar"
    assert unescape_sql_string(b"'foo\\xc3\\xa9'").decode("UTF-8") == "fooé"


def test_unescape_sql_string_named_escaped() -> None:
    assert unescape_sql_string(b"'fo\\bo'") == b"fo\bo"
    assert unescape_sql_string(b"'fo\\fo'") == b"fo\fo"
    assert unescape_sql_string(b"'fo\\ro'") == b"fo\ro"
    assert unescape_sql_string(b"'fo\\no'") == b"fo\no"
    assert unescape_sql_string(b"'fo\\to'") == b"fo\to"
    assert unescape_sql_string(b"'fo\\0o'") == b"fo\0o"


def test_unescape_sql_string_invalid() -> None:
    with pytest.raises(ValueError, match="Not a valid sql string: not enclosed in quotes"):
        assert unescape_sql_string(b"")
    with pytest.raises(ValueError, match="Not a valid sql string: not enclosed in quotes"):
        assert unescape_sql_string(b"foo")
    with pytest.raises(ValueError, match="Not a valid sql string: not enclosed in quotes"):
        assert unescape_sql_string(b"'")
    with pytest.raises(ValueError, match="Not a valid sql string: unescaped quote"):
        assert unescape_sql_string(b"'foo'bar'")
    with pytest.raises(ValueError, match="Not a valid sql string: unescaped backslash"):
        assert unescape_sql_string(b"'foo\\'")
    with pytest.raises(ValueError, match="Not a valid sql string: incomplete escape sequence"):
        assert unescape_sql_string(b"'foo\\x0'")


def test_unescape_sql_string_invalid_utf8_does_not_raise_error() -> None:
    # Since we keep the output as bytes, no errors
    assert unescape_sql_string(b"'foo\\x80'") == b"foo\x80"
    assert unescape_sql_string(b"'foo\\xc3'") == b"foo\xc3"
