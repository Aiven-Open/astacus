"""Copyright (c) 2022 Aiven Ltd
See LICENSE for details.

Hot-reloading of astacus configuration
"""

from pathlib import Path
from tests.system.conftest import astacus_ls, astacus_run, create_astacus_config, TestNode

import pytest


@pytest.mark.order("last")
def test_reload_config(tmp_path: Path, rootdir: str, astacus1: TestNode, astacus2: TestNode, astacus3: TestNode) -> None:
    # Update the root_globs config of the first node
    create_astacus_config(tmp_path=tmp_path, node=astacus1, plugin_config={"root_globs": ["*.foo"]})
    # Check that astacus can detect that reloading config is needed
    assert astacus_run(rootdir, astacus1, "check-reload", "--status", check=False).returncode == 1
    check_without_status = astacus_run(rootdir, astacus1, "check-reload", check=True, capture_output=True)
    assert check_without_status.returncode == 0
    assert "Configuration needs to be reloaded" in check_without_status.stdout.decode()
    # Reload
    astacus_run(rootdir, astacus1, "reload")
    # And now reload isn't needed anymore
    assert astacus_run(rootdir, astacus1, "check-reload", check=False).returncode == 0
    check_without_status = astacus_run(rootdir, astacus1, "check-reload", check=True, capture_output=True)
    assert check_without_status.returncode == 0
    assert "Configuration does not need to be reloaded" in check_without_status.stdout.decode()
    assert astacus1.root_path
    # Write some data to backup, including files that don't match the reloaded glob
    (astacus1.root_path / "saved.foo").write_text("dont_care")
    (astacus1.root_path / "ignored.bar").write_text("dont_care")
    # Backup
    astacus_run(rootdir, astacus1, "backup")
    # Remove all files
    (astacus1.root_path / "saved.foo").unlink()
    (astacus1.root_path / "ignored.bar").unlink()
    # Restore and confirm that only the files matching the reloaded glob are present
    astacus_run(rootdir, astacus1, "restore")
    assert astacus_ls(astacus1) == ["saved.foo"]
