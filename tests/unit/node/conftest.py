"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from astacus.common import magic
from astacus.common.snapshot import SnapshotGroup
from astacus.common.storage import FileStorage, ThreadLocalStorage
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
def fixture_app(tmp_path: Path) -> FastAPI:
    app = FastAPI()
    app.include_router(node_router, prefix="/node", tags=["node"])
    root = tmp_path / "root"
    db_path = tmp_path / "db_path"
    backup_root = tmp_path / "backup-root"
    backup_root.mkdir()
    tmp_path = tmp_path / "backup-tmp"
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
    return Uploader(thread_local_storage=ThreadLocalStorage(storage=storage))


@pytest.fixture(name="storage")
def fixture_storage(tmp_path: Path) -> FileStorage:
    storage_path = tmp_path / "storage"
    storage_path.mkdir()
    return FileStorage(storage_path)


@pytest.fixture(name="root")
def fixture_root(tmp_path: Path) -> Path:
    return tmp_path


@pytest.fixture(name="src")
def fixture_src(tmp_path: Path) -> Path:
    src = tmp_path / "src"
    src.mkdir()
    return src


@pytest.fixture(name="dst")
def fixture_dst(tmp_path: Path) -> Path:
    dst = tmp_path / "dst"
    dst.mkdir()
    return dst


@pytest.fixture(name="db")
def fixture_db(tmp_path: Path) -> Path:
    db = tmp_path / "db"
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


def create_files_at_path(dir_: Path, files: list[tuple[str, bytes]]) -> None:
    for relpath, content in files:
        path = dir_ / relpath
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            path.unlink()
        path.write_bytes(content)
