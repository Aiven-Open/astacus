from astacus.common.exceptions import NotFoundException
from astacus.common.storage.memory import MemoryStorage
from astacus.common.storage.node import NodeManifestStore
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pytest


@pytest.fixture(name="node_manifest_store")
def fixture_node_manifest_store(tmp_path: Path) -> NodeManifestStore:
    storage = MemoryStorage()
    return NodeManifestStore(storage=storage, tmp=tmp_path)


@pytest.fixture(name="pyarrow_table")
def fixture_pyarrow_table() -> pa.Table:
    return pa.Table.from_pylist(
        [
            {"a": 1, "b": 2},
            {"a": 3, "b": 4},
        ]
    )


def test_upload_download(node_manifest_store: NodeManifestStore, pyarrow_table: pa.Table) -> None:
    dataset = ds.dataset(pyarrow_table)
    node_manifest_store.upload_manifest("test", dataset)
    node_manifest_store.download_manifest("test").to_table().equals(pyarrow_table)


def test_list_manifests(node_manifest_store: NodeManifestStore, pyarrow_table: pa.Table) -> None:
    dataset = ds.dataset(pyarrow_table)
    node_manifest_store.upload_manifest("test", dataset)
    node_manifest_store.upload_manifest("test2", dataset)
    assert node_manifest_store.list_manifests() == ["test", "test2"]


def test_delete_manifest(node_manifest_store: NodeManifestStore, pyarrow_table: pa.Table) -> None:
    dataset = ds.dataset(pyarrow_table)
    node_manifest_store.upload_manifest("test", dataset)
    node_manifest_store.delete_manifest("test")
    assert node_manifest_store.list_manifests() == []
    with pytest.raises(NotFoundException):
        node_manifest_store.download_manifest("test")
