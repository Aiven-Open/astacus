"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Basic backup-restore cycle and its variations

"""

from astacus.common import magic
from tests.system.conftest import astacus_ls, astacus_run, TestNode

import logging
import pytest
import shutil

logger = logging.getLogger(__name__)


A1_FILES_AND_CONTENTS = [
    ("empty", ""),
    ("file1", "content1"),
    ("file2", "content2"),
    ("big1", "foobar" * magic.DEFAULT_EMBEDDED_FILE_SIZE),
    ("big2", "foobar" * magic.DEFAULT_EMBEDDED_FILE_SIZE),
]


# This test is the slowest, so rather fail fast in real unittests before getting here
@pytest.mark.order("last")
def test_astacus(astacus1: TestNode, astacus2: TestNode, astacus3: TestNode, rootdir: str) -> None:
    assert astacus1.root_path
    assert astacus2.root_path
    assert astacus3.root_path
    # Idea:
    # Store following files
    # a1 - A1_FILES_AND_CONTENTS
    # a2 - file1
    # a3 - (no files)
    for name, content in A1_FILES_AND_CONTENTS:
        (astacus1.root_path / name).write_text(content)
    file1_2_path = astacus2.root_path / "file1"
    file1_2_path.write_text("content1")

    astacus_run(rootdir, astacus1, "backup")

    # Clear node 1, add file that did not exist at time of backup
    shutil.rmtree(astacus1.root_path)
    astacus1.root_path.mkdir()
    file3_path = astacus1.root_path / "file3"
    file3_path.write_text("content3")

    # Ensure 'list' command does not crash (output validation is bit too painful)
    astacus_run(rootdir, astacus1, "list")

    astacus_run(rootdir, astacus1, "cleanup", "--minimum-backups", "7", "--maximum-backups", "42", "--keep-days", "15")
    astacus_run(rootdir, astacus1, "delete", "--backups", "nonexistent-is-ok")

    # Run restore
    astacus_run(rootdir, astacus2, "restore")

    # Should have now:
    # a1 - A1_FILES_AND_CONTENTS
    # a2 - file1
    # a3 - (no files)
    for name, content in A1_FILES_AND_CONTENTS:
        assert (astacus1.root_path / name).read_text() == content
    assert file1_2_path.read_text() == "content1"
    assert astacus_ls(astacus1) == sorted(x[0] for x in A1_FILES_AND_CONTENTS)
    assert astacus_ls(astacus2) == ["file1"]
    assert astacus_ls(astacus3) == []
