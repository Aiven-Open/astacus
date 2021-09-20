"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from astacus.common.utils import build_netloc, httpx_request
from typing import Dict, Iterable, List, Optional, Sequence, Union

import copy
import logging
import re
import urllib.parse

Row = Sequence[Union[str, int, float, None]]

logger = logging.getLogger(__name__)


class ClickHouseClient:
    async def execute(self, query: str) -> Iterable[Row]:
        raise NotImplementedError


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
        username: Optional[str] = None,
        password: Optional[str] = None,
        timeout: float = 10.0
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.timeout = timeout

    async def execute(self, query: str) -> Iterable[Row]:
        # Output format: https://clickhouse.tech/docs/en/interfaces/formats/#jsoncompact
        headers = [("X-ClickHouse-Database", "system"), ("X-ClickHouse-Format", "JSONCompact")]
        if self.username is not None:
            headers.append(("X-ClickHouse-User", self.username))
        if self.password is not None:
            headers.append(("X-ClickHouse-Key", self.password))
        netloc = build_netloc(self.host, self.port)
        response = await httpx_request(
            url=urllib.parse.urlunsplit(("http", netloc, "", None, None)),
            params={"query": query},
            method="post",
            # The response can be empty so we can't use httpx_request builtin decoding
            json=False,
            caller="ClickHouseClient",
            headers=headers,
            timeout=self.timeout,
        )
        assert not isinstance(response, dict)
        if response is None:
            # We should find a better way to handler failure than the None from httpx_request
            raise Exception(f"Query failed: {query!r} on {self.host}:{self.port}")
        if response.content:
            # Beware: large ints (Int64, UInt64, and larger) will be returned as strings
            decoded_response = response.json()
            return decoded_response["data"]
        return []


class StubClickHouseClient(ClickHouseClient):
    def __init__(self) -> None:
        self.responses: Dict[str, List[Row]] = {}

    def set_response(self, query: str, rows: List[Row]) -> None:
        self.responses[query] = copy.deepcopy(rows)

    async def execute(self, query: str) -> Iterable[Row]:
        return copy.deepcopy(self.responses[query])


def escape_sql_identifier(identifier: str) -> str:
    """
    Escapes backticks and backslashes with a backslash and wraps everything between backticks.
    """
    # The reference only specifies when escaping is not necessary, but does not completely
    # specifies how the escaping is done and which character are allowed.
    # We escape all identifiers. We could try to escape only the identifiers that conflict with keywords,
    # but that requires knowing the set of all ClickHouse keywords, which is very likely to change.
    # ref: https://clickhouse.tech/docs/en/sql-reference/syntax/#syntax-identifiers
    escaped_identifier = re.sub(r"([`\\])", r"\\\1", identifier)
    return f"`{escaped_identifier}`"


def escape_sql_string(string: str) -> str:
    """
    Escapes single quotes and backslashes with a backslash and wraps everything between single quotes.
    """
    escaped_identifier = re.sub(r"(['\\])", r"\\\1", string)
    return f"'{escaped_identifier}'"
