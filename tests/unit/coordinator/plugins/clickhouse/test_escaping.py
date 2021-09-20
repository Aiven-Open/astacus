"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from astacus.coordinator.plugins.clickhouse.escaping import escape_for_file_name, unescape_from_file_name

import pytest
import re

pytestmark = [pytest.mark.clickhouse]


def test_escape_for_file_name_keeps_alphanumeric_and_underscore() -> None:
    assert escape_for_file_name("cat") == "cat"
    assert escape_for_file_name("LARGE_CAT") == "LARGE_CAT"
    assert escape_for_file_name("cat56") == "cat56"


def test_escape_for_file_name_escapes_with_uppercase_percent_encoding() -> None:
    # A few readable samples
    assert escape_for_file_name("cat/tabby") == "cat%2Ftabby"
    assert escape_for_file_name("cat/spotted_tabby") == "cat%2Fspotted_tabby"
    assert escape_for_file_name("cat/tortoise-shell") == "cat%2Ftortoise%2Dshell"
    # An exhaustive scan of the single-byte UTF-8 (include all control characters)
    for byte_char in range(0, 128):
        char = byte_char.to_bytes(1, "little").decode()
        if not re.match(r"[a-zA-Z0-9_]", char):
            assert escape_for_file_name(char) == f"%{byte_char:02X}"


def test_escape_for_file_name_encodes_as_utf8_before_escaping() -> None:
    assert escape_for_file_name("cat/câlicö") == "cat%2Fc%C3%A2lic%C3%B6"


def test_unescape_from_file_name() -> None:
    assert unescape_from_file_name("cat%2Fc%C3%A2lic%C3%B6") == "cat/câlicö"
