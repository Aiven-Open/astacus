"""

Copyright (c) 2023 Aiven Ltd
See LICENSE for details

Classes for working with snapshot groups.

"""
from astacus.common.snapshot import SnapshotGroup
from pathlib import Path
from typing import Iterable, Optional, Sequence
from typing_extensions import Self
from wcmatch.glob import GLOBSTAR, iglob, translate

import dataclasses
import os
import re

WCMATCH_FLAGS = GLOBSTAR


@dataclasses.dataclass
class CompiledGroup:
    group: SnapshotGroup
    regex: re.Pattern

    @classmethod
    def compile(cls, group: SnapshotGroup) -> Self:
        return cls(group, glob_compile(group.root_glob))

    def matches(self, relative_path: Path) -> bool:
        return bool(self.regex.match(str(relative_path))) and relative_path.name not in self.group.excluded_names

    def glob(self, root_dir: Optional[Path] = None) -> Iterable[str]:
        for path in iglob(self.group.root_glob, root_dir=root_dir, flags=WCMATCH_FLAGS):
            if os.path.basename(path) not in self.group.excluded_names:
                yield path


@dataclasses.dataclass
class CompiledGroups:
    groups: Sequence[CompiledGroup]

    @classmethod
    def compile(cls, groups: Sequence[SnapshotGroup]) -> Self:
        return cls([CompiledGroup.compile(group) for group in groups])

    def get_matching(self, relative_path: Path) -> list[SnapshotGroup]:
        return [group.group for group in self.groups if group.matches(relative_path)]

    def any_match(self, relative_path: Path) -> bool:
        return any(group.matches(relative_path) for group in self.groups)

    def root_globs(self) -> list[str]:
        return [group.group.root_glob for group in self.groups]


def glob_compile(glob: str) -> re.Pattern:
    return re.compile(translate(glob, flags=WCMATCH_FLAGS)[0][0])
