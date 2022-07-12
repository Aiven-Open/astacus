"""
Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""
from astacus.coordinator.plugins.clickhouse.macros import MacroExpansionError, Macros

import pytest


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
