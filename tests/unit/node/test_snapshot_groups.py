"""

Copyright (c) 2023 Aiven Ltd
See LICENSE for details

"""
from astacus.common.snapshot import SnapshotGroup
from astacus.node.snapshot_groups import CompiledGroup, CompiledGroups, glob_compile
from pathlib import Path

import os

POSITIVE_TEST_CASES: list[tuple[Path, str]] = [
    (Path("foo"), "foo"),
    (Path("foo"), "*"),
    (Path("foo/bar"), "*/bar"),
    (Path("foo"), "**"),
    (Path("foo/bar"), "**"),
    (Path("foo/bar/baz"), "**/*"),
    (Path("foo/bar"), "**/*"),
    (Path("foo/bar"), "**/**"),
]

NEGATIVE_TEST_CASES: list[tuple[Path, str]] = [
    (Path("foo/bar/baz"), "*/*"),
    (Path("foo"), "foobar"),
    (Path("foo"), "*/foo"),
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
        p = tmp_path / p
        p.mkdir(parents=True, exist_ok=True)
        p.touch()
    for p, glob in POSITIVE_TEST_CASES:
        group = SnapshotGroup(root_glob=glob)
        assert str(p) in CompiledGroup.compile(group).glob(tmp_path)
    for p, glob in NEGATIVE_TEST_CASES:
        group = SnapshotGroup(root_glob=glob)
        assert str(p) not in CompiledGroup.compile(group).glob(tmp_path)
