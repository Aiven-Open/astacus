"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from astacus.common.hashstorage import FileHashStorage
from astacus.common.progress import Progress
from astacus.node.api import router as node_router
from astacus.node.config import NodeConfig
from astacus.node.snapshot import Snapshotter
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pathlib import Path

import pytest


@pytest.fixture(name="app")
def fixture_app(tmpdir):
    app = FastAPI()
    app.include_router(node_router, prefix="/node", tags=["node"])
    root = Path(tmpdir) / "root"
    backup_root = Path(tmpdir) / "backup-root"
    root.mkdir()
    (root / "foo").write_text("foobar")
    (root / "foo2").write_text("foobar")
    root_link = Path(tmpdir) / "root-link"
    app.state.node_config = NodeConfig(
        root=str(root), root_link=str(root_link), root_globs=["*"], backup_root=str(backup_root)
    )
    yield app


@pytest.fixture(name="client")
def fixture_client(app):
    yield TestClient(app)


class SnapshotterWithDefaults(Snapshotter):
    def create_2foobar(self):
        (self.src / "foo").write_text("foobar")
        (self.src / "foo2").write_text("foobar")
        progress = Progress()
        assert self.snapshot(progress=progress) > 0
        assert progress.finished_successfully
        ss1 = self.get_snapshot_state()
        assert self.snapshot(progress=Progress()) == 0
        ss2 = self.get_snapshot_state()
        assert ss1 == ss2


@pytest.fixture(name="snapshotter")
def fixture_snapshotter(tmpdir):
    src = Path(tmpdir) / "src"
    src.mkdir()
    dst = Path(tmpdir) / "dst"
    dst.mkdir()
    yield SnapshotterWithDefaults(src=src, dst=dst, globs=["*"])


@pytest.fixture(name="storage")
def fixture_storage(tmpdir):
    storage_path = Path(tmpdir) / "storage"
    storage_path.mkdir()
    yield FileHashStorage(storage_path)
