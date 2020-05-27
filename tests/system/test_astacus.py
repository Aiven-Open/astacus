"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Basic backup-restore cycle and its variations

"""

from astacus.common.utils import exponential_backoff

import logging
import pytest
import requests
import shutil

logger = logging.getLogger(__name__)


def _astacus_run(astacus, op):
    r = requests.post(f"{astacus.url}/{op}")
    assert r and r.ok
    url = r.json()["status_url"]
    for _ in exponential_backoff(initial=0.1, duration=10):
        logger.debug("Checking {op} status at %r", url)
        r = requests.get(url)
        assert r and r.ok
        state = r.json()["state"]
        assert state != "fail"
        if state == "done":
            break
    else:
        raise TimeoutError("_astacus_run timed out")


def _astacus_ls(astacus):
    return sorted(str(x.relative_to(astacus.root_path)) for x in astacus.root_path.glob("**/*"))


@pytest.mark.asyncio
async def test_astacus(astacus1, astacus2, astacus3):
    # Idea:
    # Store following files
    # a1 - file1, file2
    # a2 - file1
    # a3 - empty
    file1_path = astacus1.root_path / "file1"
    file1_2_path = astacus2.root_path / "file1"
    file2_path = astacus1.root_path / "file2"
    file3_path = astacus1.root_path / "file3"
    file1_path.write_text("content1")
    file1_2_path.write_text("content1")
    file2_path.write_text("content2")

    _astacus_run(astacus1, "backup")

    # Clear node 1, add file that did not exist at time of backup
    shutil.rmtree(astacus1.root_path)
    astacus1.root_path.mkdir()
    file3_path.write_text("content3")

    # Run restore
    _astacus_run(astacus2, "restore")

    # Should have now:
    # a1 - file1, file2
    # a2 - file1
    # a3 - nothing
    assert file1_path.read_text() == "content1"
    assert file2_path.read_text() == "content2"
    assert not file3_path.is_file()
    assert file1_2_path.read_text() == "content1"
    assert _astacus_ls(astacus1) == ["file1", "file2"]
    assert _astacus_ls(astacus2) == ["file1"]
    assert _astacus_ls(astacus3) == []
