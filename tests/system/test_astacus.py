"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Basic backup-restore cycle and its variations

"""

from astacus.common import magic

import logging
import pytest
import shutil
import subprocess
import sys

logger = logging.getLogger(__name__)


def _astacus_ls(astacus):
    return sorted(str(x.relative_to(astacus.root_path)) for x in astacus.root_path.glob("**/*"))


A1_FILES_AND_CONTENTS = [
    ("empty", ""),
    ("file1", "content1"),
    ("file2", "content2"),
    ("big1", "foobar" * magic.EMBEDDED_FILE_SIZE),
    ("big2", "foobar" * magic.EMBEDDED_FILE_SIZE),
]


# This test is the slowest, so rather fail fast in real unittests before getting here
@pytest.mark.order("last")
@pytest.mark.asyncio
async def test_astacus(astacus1, astacus2, astacus3, rootdir):
    def astacus_run(astacus, *args):
        # simulate this (for some reason, in podman the 'astacus' command
        # is not to be found, I suppose the package hasn't been
        # initialized)
        #
        # cmd = ["astacus", "--url", astacus.url, "-w", "10"]
        cmd = [sys.executable, "-m", "astacus.main", "--url", astacus.url, "-w", "10"]
        subprocess.run(cmd + list(args), check=True, env={"PYTHONPATH": rootdir})

    # Idea:
    # Store following files
    # a1 - A1_FILES_AND_CONTENTS
    # a2 - file1
    # a3 - (no files)
    for name, content in A1_FILES_AND_CONTENTS:
        (astacus1.root_path / name).write_text(content)
    file1_2_path = astacus2.root_path / "file1"
    file1_2_path.write_text("content1")

    astacus_run(astacus1, "backup")

    # Clear node 1, add file that did not exist at time of backup
    shutil.rmtree(astacus1.root_path)
    astacus1.root_path.mkdir()
    file3_path = astacus1.root_path / "file3"
    file3_path.write_text("content3")

    # Ensure 'list' command does not crash (output validation is bit too painful)
    astacus_run(astacus1, "list")

    astacus_run(astacus1, "cleanup", "--minimum-backups", "7", "--maximum-backups", "42", "--keep-days", "15")
    astacus_run(astacus1, "delete", "--backups", "nonexistent-is-ok")

    # Run restore
    astacus_run(astacus2, "restore")

    # Should have now:
    # a1 - A1_FILES_AND_CONTENTS
    # a2 - file1
    # a3 - (no files)
    for name, content in A1_FILES_AND_CONTENTS:
        assert (astacus1.root_path / name).read_text() == content
    assert file1_2_path.read_text() == "content1"
    assert _astacus_ls(astacus1) == sorted(x[0] for x in A1_FILES_AND_CONTENTS)
    assert _astacus_ls(astacus2) == ["file1"]
    assert _astacus_ls(astacus3) == []
