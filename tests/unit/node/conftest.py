"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from astacus.common import magic
from astacus.common.snapshot import SnapshotGroup
from astacus.common.storage import FileStorage
from astacus.node.api import router as node_router
from astacus.node.config import NodeConfig
from astacus.node.snapshot import Snapshot
from astacus.node.snapshotter import Snapshotter
from astacus.node.sqlite_snapshot import SQLiteSnapshot, SQLiteSnapshotter
from astacus.node.uploader import Uploader
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pathlib import Path

import pytest


@pytest.fixture(name="app")
def fixture_app(tmpdir) -> FastAPI:
    app = FastAPI()
    app.include_router(node_router, prefix="/node", tags=["node"])
    root = Path(tmpdir) / "root"
    db_path = Path(tmpdir) / "db_path"
    backup_root = Path(tmpdir) / "backup-root"
    backup_root.mkdir()
    tmp_path = Path(tmpdir) / "backup-tmp"
    root.mkdir()
    (root / "foo").write_text("foobar")
    (root / "foo2").write_text("foobar")
    (root / "foobig").write_text("foobar" * magic.DEFAULT_EMBEDDED_FILE_SIZE)
    (root / "foobig2").write_text("foobar" * magic.DEFAULT_EMBEDDED_FILE_SIZE)
    app.state.node_config = NodeConfig.parse_obj(
        {
            "az": "testaz",
            "root": str(root),
            "db_path": str(db_path),
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
    return app


@pytest.fixture(name="client")
def fixture_client(app) -> TestClient:
    return TestClient(app)


@pytest.fixture(name="uploader")
def fixture_uploader(storage):
    return Uploader(storage=storage)


@pytest.fixture(name="storage")
def fixture_storage(tmpdir) -> FileStorage:
    storage_path = Path(tmpdir) / "storage"
    storage_path.mkdir()
    return FileStorage(storage_path)


@pytest.fixture(name="root")
def fixture_root(tmpdir) -> Path:
    return Path(tmpdir)


@pytest.fixture(name="src")
def fixture_src(tmpdir) -> Path:
    src = Path(tmpdir) / "src"
    src.mkdir()
    return src


@pytest.fixture(name="dst")
def fixture_dst(tmpdir) -> Path:
    dst = Path(tmpdir) / "dst"
    dst.mkdir()
    return dst


@pytest.fixture(name="db")
def fixture_db(tmpdir) -> Path:
    db = Path(tmpdir) / "db"
    return db


def build_snapshot_and_snapshotter(
    src: Path,
    dst: Path,
    db: Path,
    snapshot_cls: type[Snapshot],
    groups: list[SnapshotGroup],
) -> tuple[Snapshot, Snapshotter]:
    if snapshot_cls is SQLiteSnapshot:
        snapshot = SQLiteSnapshot(dst, db)
        snapshotter = SQLiteSnapshotter(src=src, dst=dst, snapshot=snapshot, groups=groups, parallel=2)
    else:
        assert False
    return snapshot, snapshotter


def create_files_at_path(dir_: Path, files: list[tuple[Path, bytes]]) -> None:
    for relpath, content in files:
        path = dir_ / relpath
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            path.unlink()
        path.write_bytes(content)
