"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""

from astacus.common.utils import build_netloc, httpx_request
from collections.abc import Mapping, Sequence
from re import Match

import copy
import logging
import re
import urllib.parse

Row = Sequence[str | int | float | list | None]

logger = logging.getLogger(__name__)


class ClickHouseClient:
    async def execute(self, query: bytes, timeout: float | None = None, session_id: str | None = None) -> Sequence[Row]:
        raise NotImplementedError


class ClickHouseClientQueryError(Exception):
    # If we have to handle more error types, we might consider adding proper
    # rich error types and not display numeric values to the end user.
    UNKNOWN_SETTING = 115
    SETTING_CONSTRAINT_VIOLATION = 452

    def __init__(
        self,
        query: bytes,
        host: str,
        port: int,
        *,
        status_code: int | None = None,
        exception_code: int | None = None,
        response: bytes | None = None,
    ) -> None:
        super().__init__(f"Query failed: {query!r} on {host}:{port}: {status_code=}, {exception_code=}, {response=}")
        self.query = query
        self.host = host
        self.port = port
        self.status_code = status_code
        self.exception_code = exception_code


class HttpClickHouseClient(ClickHouseClient):
    """
    ClickHouse client using the HTTP(S) protocol, the port provided
    should match the `http_port` or `https_port` server option.

    The client forces all connections to the `system` database, this
    is desirable since we know this database always exists and should
    never be part of any backup.
    """

    def __init__(
        self,
        *,
        host: str,
        port: int,
        username: str | None = None,
        password: str | None = None,
        timeout: float = 10.0,
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.timeout = timeout

    async def execute(self, query: bytes, timeout: float | None = None, session_id: str | None = None) -> Sequence[Row]:
        assert isinstance(query, bytes)
        # Output format: https://clickhouse.tech/docs/en/interfaces/formats/#jsoncompact
        headers = [("X-ClickHouse-Database", "system"), ("X-ClickHouse-Format", "JSONCompact")]
        if self.username is not None:
            headers.append(("X-ClickHouse-User", self.username))
        if self.password is not None:
            headers.append(("X-ClickHouse-Key", self.password))
        netloc = build_netloc(self.host, self.port)
        params = {"wait_end_of_query": "1"}
        if session_id is not None:
            params["session_id"] = session_id
        response = await httpx_request(
            url=urllib.parse.urlunsplit(("http", netloc, "", None, None)),
            params=params,
            method="post",
            content=query,
            # The response can be empty so we can't use httpx_request builtin decoding
            json=False,
            caller="ClickHouseClient",
            headers=headers,
            timeout=self.timeout if timeout is None else timeout,
            ignore_status_code=True,
        )
        assert not isinstance(response, Mapping)
        if response is None:
            # We should find a better way to handler failure than the None from httpx_request
            raise ClickHouseClientQueryError(query, self.host, self.port)
        if response.is_error:
            exception_code = None
            raw_exception_code = response.headers.get("x-clickhouse-exception-code")
            if isinstance(raw_exception_code, str) and re.match(r"\d+", raw_exception_code):
                exception_code = int(raw_exception_code)
            raise ClickHouseClientQueryError(
                query,
                self.host,
                self.port,
                status_code=response.status_code,
                exception_code=exception_code,
                response=response.content,
            )
        if response.content:
            # Beware: large ints (Int64, UInt64, and larger) will be returned as strings
            decoded_response = response.json()
            return decoded_response["data"]
        return []


class StubClickHouseClient(ClickHouseClient):
    def __init__(self) -> None:
        self.responses: dict[bytes, Sequence[Row]] = {}

    def set_response(self, query: bytes, rows: Sequence[Row]) -> None:
        self.responses[query] = copy.deepcopy(rows)

    async def execute(self, query: bytes, timeout: float | None = None, session_id: str | None = None) -> Sequence[Row]:
        assert isinstance(query, bytes)
        return copy.deepcopy(self.responses[query])


IDENTIFIER_ESCAPE_MAP = {
    b"\b"[0]: b"\\b",
    b"\f"[0]: b"\\f",
    b"\r"[0]: b"\\r",
    b"\n"[0]: b"\\n",
    b"\t"[0]: b"\\t",
    b"\0"[0]: b"\\0",
    b"\\"[0]: b"\\\\",
    b"`"[0]: b"\\`",
}

STRING_ESCAPE_MAP = {
    b"\b"[0]: b"\\b",
    b"\f"[0]: b"\\f",
    b"\r"[0]: b"\\r",
    b"\n"[0]: b"\\n",
    b"\t"[0]: b"\\t",
    b"\0"[0]: b"\\0",
    b"\\"[0]: b"\\\\",
    b"'"[0]: b"\\'",
}


def escape_sql_identifier(identifier: bytes) -> str:
    r"""Escape a byte string into an sql identifier usable in a ClickHouse query.

    Backtick, backslashes and \b \f \r \n \t \0 are escaped with a backslash.

    Other special bytes (anything below 0x20 or greater than 0x7e) are escaped
    with a \xHH sequence.

    The entire value is wrapped in backticks, even when unnecessary :
    We could try to escape only the identifiers that conflict with keywords,
    but that requires knowing the set of all ClickHouse keywords, which is very
    likely to change.
    ref: https://clickhouse.com/docs/en/sql-reference/syntax/#syntax-identifiers
    """
    return _escape_bytes(identifier, IDENTIFIER_ESCAPE_MAP, b"`")


def escape_sql_string(string: bytes) -> str:
    """
    Escapes single quotes and backslashes with a backslash and wraps everything between single quotes.
    """
    return _escape_bytes(string, STRING_ESCAPE_MAP, b"'")


def _escape_bytes(value: bytes, escape_map: Mapping[int, bytes], quote_char: bytes) -> str:
    buffer = [quote_char]
    for byte in value:
        escape_sequence = escape_map.get(byte)
        if escape_sequence is not None:
            buffer.append(escape_sequence)
        elif byte < 0x20 or byte > 0x7E:
            buffer.append(f"\\x{byte:02x}".encode())
        else:
            buffer.append(bytes((byte,)))
    buffer.append(quote_char)
    return b"".join(buffer).decode()


def unescape_sql_string(string: bytes) -> bytes:
    """Parse a ClickHouse-escaped SQL string.

    Since ClickHouse does not have any text encoding and their string can contain anything,
    keep the output as bytes and don't attempt UTF-8 decoding.
    """
    if len(string) >= 2 and string[:1] == b"'" and string[-1:] == b"'":
        # The last four alternatives of the regex look weird, "anything but X" or "X":
        # This is used to detect incomplete \x escape sequences, unescaped backslashes or quotes (see tests).
        return re.sub(rb"\\x([0-9a-fA-F]{2})|\\([^x])|(\\x)|([^\\'])|([\\'])", _unescape_sql_char, string[1:-1])
    raise ValueError("Not a valid sql string: not enclosed in quotes")


def _unescape_sql_char(match: Match) -> bytes:
    if match.group(1):
        return int(match.group(1), base=16).to_bytes(1, "little")
    if match.group(2):
        return match.group(2).translate(bytes.maketrans(b"bfrnt0", b"\b\f\r\n\t\0"))  # cspell: disable-line
    if match.group(3):
        raise ValueError("Not a valid sql string: incomplete escape sequence")
    if match.group(4):
        return match.group(4)
    unescaped_char = {b"'": "quote", b"\\": "backslash"}[match.group(5)]
    raise ValueError(f"Not a valid sql string: unescaped {unescaped_char}")
