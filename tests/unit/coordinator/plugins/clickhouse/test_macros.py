"""Copyright (c) 2022 Aiven Ltd
See LICENSE for details.
"""

from astacus.coordinator.plugins.clickhouse.client import StubClickHouseClient
from astacus.coordinator.plugins.clickhouse.macros import fetch_server_macros, MacroExpansionError, Macros, MACROS_LIST_QUERY
from base64 import b64encode

import pytest


def b64_str(b: bytes) -> str:
    return b64encode(b).decode()


@pytest.fixture(name="macros")
def fixture_macros() -> Macros:
    return Macros()


def test_simple_expansion(macros: Macros) -> None:
    macros.add(b"shard", b"all_nodes")
    assert macros.expand(b"things/{shard}") == b"things/all_nodes"


def test_missing_macro(macros: Macros) -> None:
    with pytest.raises(MacroExpansionError, match="No macro named b'shard'"):
        macros.expand(b"things/{shard}")


def test_unbalanced_opening_bracket_fails(macros: Macros) -> None:
    with pytest.raises(MacroExpansionError, match="Unbalanced { and } in string with macros"):
        macros.expand(b"things/{shard")


def test_unbalanced_opening_bracket_inside_macro_is_part_of_the_name(macros: Macros) -> None:
    macros.add(b"{shard", b"weird_shard")
    assert macros.expand(b"things/{{shard}") == b"things/weird_shard"


def test_unbalanced_closing_bracket_is_ignored(macros: Macros) -> None:
    assert macros.expand(b"things/shard}") == b"things/shard}"


def test_closing_bracket_matching_is_non_greedy(macros: Macros) -> None:
    macros.add(b"sha", b"md5")
    assert macros.expand(b"things/{sha}rd}") == b"things/md5rd}"


def test_closing_bracket_matching_is_non_greedy_without_matching_macro(macros: Macros) -> None:
    with pytest.raises(MacroExpansionError, match="No macro named b'sha'"):
        assert macros.expand(b"things/{sha}rd}")


def test_multiple_expansions(macros: Macros) -> None:
    macros.add(b"foo", b"123")
    macros.add(b"bar", b"456")
    assert macros.expand(b"{foo}+{bar}={foo}{bar}") == b"123+456=123456"


def test_recursive_expansion(macros: Macros) -> None:
    macros.add(b"a", b"{b}{b}")
    macros.add(b"b", b"{c}{c}")
    macros.add(b"c", b"d")
    assert macros.expand(b"{a}") == b"dddd"


def test_too_deep_recursion(macros: Macros) -> None:
    macros.add(b"foo", b"{foo}")
    with pytest.raises(MacroExpansionError, match="Too deep recursion while expanding macros"):
        macros.expand(b"{foo}")


def test_too_long_expansion(macros: Macros) -> None:
    macros.add(b"a", b"{b}{b}{b}{b}{b}{b}{b}{b}{b}{b}{b}{b}{b}{b}{b}{b}")
    macros.add(b"b", b"{c}{c}{c}{c}{c}{c}{c}{c}{c}{c}{c}{c}{c}{c}{c}{c}{c}{c}")
    macros.add(b"c", b"{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}")
    macros.add(b"d", b"eeeeeeeeeeeeeeee")
    with pytest.raises(MacroExpansionError, match="Too long string while expanding macros"):
        macros.expand(b"{a}")


def test_from_mapping() -> None:
    macros_from_mapping = Macros.from_mapping({b"foo": b"123", b"bar": b"456"})
    expected_macros = Macros()
    expected_macros.add(b"foo", b"123")
    expected_macros.add(b"bar", b"456")
    assert macros_from_mapping == expected_macros


def test_as_mapping() -> None:
    macros = Macros()
    macros.add(b"foo", b"123")
    macros.add(b"bar", b"456")
    assert macros.as_mapping() == {b"foo": b"123", b"bar": b"456"}


async def test_fetch_server_macros() -> None:
    client = StubClickHouseClient()
    client.set_response(
        MACROS_LIST_QUERY,
        [
            [b64_str(b"shard_group_a"), b64_str(b"a1")],
            [b64_str(b"shard_group_b"), b64_str(b"b1")],
            [b64_str(b"replica"), b64_str(b"node_1")],
            [b64_str(b"dont\x80care"), b64_str(b"its\x00fine")],
        ],
    )
    server_macros = await fetch_server_macros(client)
    assert server_macros.as_mapping() == {
        b"shard_group_a": b"a1",
        b"shard_group_b": b"b1",
        b"replica": b"node_1",
        b"dont\x80care": b"its\x00fine",
    }
