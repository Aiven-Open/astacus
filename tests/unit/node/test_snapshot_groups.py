"""Copyright (c) 2023 Aiven Ltd
See LICENSE for details.

"""

from astacus.common.snapshot import SnapshotGroup
from astacus.node.snapshot_groups import CompiledGroup, CompiledGroups, glob_compile
from pathlib import Path

import os

POSITIVE_TEST_CASES: list[tuple[str, str]] = [
    ("foo", "foo"),
    ("foo", "*"),
    ("foo/bar", "*/bar"),
    ("foo", "**"),
    ("foo/bar", "**"),
    ("foo/bar/baz", "**/*"),
    ("foo/bar", "**/*"),
    ("foo/bar", "**/**"),
]

NEGATIVE_TEST_CASES: list[tuple[str, str]] = [
    ("foo/bar/baz", "*/*"),
    ("foo", "foobar"),
    ("foo", "*/foo"),
]


def test_compile() -> None:
    for path, glob in POSITIVE_TEST_CASES:
        assert glob_compile(glob).match(str(path)) is not None
    for path, glob in NEGATIVE_TEST_CASES:
        assert glob_compile(glob).match(str(path)) is None


def test_CompiledGroup_matches() -> None:
    for path, glob in POSITIVE_TEST_CASES:
        group = SnapshotGroup(root_glob=glob)
        assert CompiledGroup.compile(group).matches(path)
        group = SnapshotGroup(root_glob=glob, excluded_names=[os.path.basename(path)])
        assert not CompiledGroup.compile(group).matches(path)
    for path, glob in NEGATIVE_TEST_CASES:
        group = SnapshotGroup(root_glob=glob)
        assert not CompiledGroup.compile(group).matches(path)


def test_CompiledGroups() -> None:
    for path, glob in POSITIVE_TEST_CASES:
        group1 = SnapshotGroup(root_glob=glob)
        group2 = SnapshotGroup(root_glob=glob, excluded_names=[os.path.basename(path)])
        group3 = SnapshotGroup(root_glob="doesntmatch")
        compiled = CompiledGroups.compile([group1, group2, group3])
        assert compiled.any_match(path)
        assert compiled.get_matching(path) == [group1]


def test_CompiledGroup_glob(tmp_path: Path) -> None:
    for p, _ in POSITIVE_TEST_CASES + NEGATIVE_TEST_CASES:
        absolute_p = tmp_path / p
        absolute_p.mkdir(parents=True, exist_ok=True)
        absolute_p.touch()
    for p, glob in POSITIVE_TEST_CASES:
        group = SnapshotGroup(root_glob=glob)
        assert str(p) in CompiledGroup.compile(group).glob(tmp_path)
    for p, glob in NEGATIVE_TEST_CASES:
        group = SnapshotGroup(root_glob=glob)
        assert str(p) not in CompiledGroup.compile(group).glob(tmp_path)
