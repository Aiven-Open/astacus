"""
Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""

from _pytest.config.argparsing import Parser
from argparse import ArgumentTypeError
from pathlib import Path

import platform
import pytest

CLICKHOUSE_PATH_OPTION = "--clickhouse-path"
CLICKHOUSE_RESTORE_PATH_OPTION = "--clickhouse-restore-path"

pytest_plugins = [
    "tests.plugins.asyncio_loop",
]


def pytest_addoption(parser: Parser) -> None:
    parser.addoption(
        CLICKHOUSE_PATH_OPTION,
        action="store",
        type=checked_path_to_file,
        default=None,
        help="Path to the ClickHouse binary used in integration tests (default: auto-detected)",
    )
    parser.addoption(
        CLICKHOUSE_RESTORE_PATH_OPTION,
        action="store",
        type=checked_path_to_file,
        default=None,
        help="Path to the ClickHouse binary used for restoring in integration tests (default: use the same as for backup)",
    )


def pytest_runtest_setup(item: pytest.Item) -> None:
    if any(item.iter_markers(name="x86_64")) and platform.machine() != "x86_64":
        pytest.skip("x86_64 arch required")


def checked_path_to_file(option_value: str) -> Path:
    """Return an absolute path to a file that exists and can be opened.

    Relative paths are converted to absolute path using the current working directory.

    Fails with an ArgumentTypeError describing the issue (file not found, not a file, etc.) if the file cannot be opened.
    """
    option_path = Path(option_value).absolute()
    try:
        with option_path.open():
            pass
    except OSError as e:
        raise ArgumentTypeError(str(e)) from e
    return option_path
