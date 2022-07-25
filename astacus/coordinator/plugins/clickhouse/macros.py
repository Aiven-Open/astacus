"""
Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""
from re import Match

import dataclasses
import re


class MacroExpansionError(Exception):
    pass


@dataclasses.dataclass(frozen=True)
class Macros:
    """ClickHouse uses macro substitution in a few places: shard definition, replica definition...

    They are useful because the value before substitution can be replicated across multiple servers
    but the macro values are defined per-server, that's how each replica can be configured
    differently while still executing the same queries everywhere.
    """

    _macros: dict[bytes, bytes] = dataclasses.field(default_factory=dict, init=False)

    def add(self, macro: bytes, substitution: bytes) -> None:
        self._macros[macro] = substitution

    def expand(self, content: bytes, *, _level: int = 0) -> bytes:
        if _level > 0 and len(content) > 65536:
            raise MacroExpansionError("Too long string while expanding macros")
        if _level >= 10:
            raise MacroExpansionError(f"Too deep recursion while expanding macros: {content!r}")
        if b"{" in content:
            new_content, substitutions_count = re.subn(rb"\{([^}]+)}", self._get_substitution, content)
            if substitutions_count > 0:
                return self.expand(new_content, _level=_level + 1)
            raise MacroExpansionError(f"Unbalanced {{ and }} in string with macros: {content!r}")
        return content

    def _get_substitution(self, matcher: Match) -> bytes:
        macro = matcher.group(1)
        if macro in self._macros:
            return self._macros[macro]
        raise MacroExpansionError(f"No macro named {macro!r}")
