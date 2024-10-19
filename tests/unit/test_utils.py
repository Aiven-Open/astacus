"""Copyright (c) 2022 Aiven Ltd
See LICENSE for details.
"""

from collections.abc import Sequence
from tests.utils import parse_clickhouse_version

import pytest


@pytest.mark.parametrize(
    ("version_command_output", "expected_version"),
    [
        (b"ClickHouse server version 22.0.0.1 (official build).", (22, 0, 0, 1)),
        (b"ClickHouse server version 22.1.2.3.", (22, 1, 2, 3)),
    ],
)
def test_parse_clickhouse_version(version_command_output: bytes, expected_version: Sequence[int]) -> None:
    assert parse_clickhouse_version(version_command_output) == expected_version
