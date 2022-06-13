"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from astacus.common import magic
from astacus.common.progress import Progress
from astacus.common.storage import FileStorage
from astacus.node.api import router as node_router
from astacus.node.config import NodeConfig
from astacus.node.snapshotter import Snapshotter
from astacus.node.uploader import Uploader
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
    backup_root.mkdir()
    tmp_path = Path(tmpdir) / "backup-tmp"
    root.mkdir()
    (root / "foo").write_text("foobar")
    (root / "foo2").write_text("foobar")
    (root / "foobig").write_text("foobar" * magic.EMBEDDED_FILE_SIZE)
    (root / "foobig2").write_text("foobar" * magic.EMBEDDED_FILE_SIZE)
    app.state.node_config = NodeConfig.parse_obj(
        {
            "az": "testaz",
            "root": str(root),
            "object_storage": {
                "temporary_directory": str(tmp_path),
                "default_storage": "x",
                "compression": {"algorithm": "zstd"},
                "storages": {
                    "x": {
                        "storage_type": "local",
                        "directory": str(backup_root),
                    }
                },
            },
        }
    )
    yield app


@pytest.fixture(name="client")
def fixture_client(app):
    yield TestClient(app)


class SnapshotterWithDefaults(Snapshotter):
    def create_4foobar(self):
        (self.src / "foo").write_text("foobar")
        (self.src / "foo2").write_text("foobar")
        (self.src / "foobig").write_text("foobar" * magic.EMBEDDED_FILE_SIZE)
        (self.src / "foobig2").write_text("foobar" * magic.EMBEDDED_FILE_SIZE)
        progress = Progress()
        assert self.snapshot(progress=progress) > 0
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
    yield SnapshotterWithDefaults(src=src, dst=dst, globs=["*"], parallel=1)


@pytest.fixture(name="uploader")
def fixture_uploader(storage):
    yield Uploader(storage=storage)


@pytest.fixture(name="storage")
def fixture_storage(tmpdir):
    storage_path = Path(tmpdir) / "storage"
    storage_path.mkdir()
    yield FileStorage(storage_path)
