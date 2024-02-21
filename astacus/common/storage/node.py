from astacus.common.storage.base import Storage
from pathlib import Path
from pyarrow.parquet import ParquetWriter
from typing import Self
import tempfile

import os
import pyarrow.dataset as ds


class NodeManifestStore:
    FORMAT = "parquet"
    ROWS_PER_GROUP = 1_000

    @classmethod
    def create_with_new_tmp_storage(cls, storage: Storage, node_id: int) -> Self:
        tmp =

    def __init__(self, storage: Storage, node_id: int, tmp: Path) -> None:
        self.tmp = tmp / str(node_id)
        os.makedirs(self.tmp, exist_ok=True)
        self.storage = storage
        self.node_id = node_id

    def download_manifest(self, name: str) -> ds.Dataset:
        file = self.tmp / name
        with open(file, "wb") as f:
            print(name)
            self.storage.download_key_to_file(self._key(name), f)
        return ds.dataset(file, format="parquet")

    def upload_manifest(self, name: str, manifest: ds.Dataset) -> None:
        file = self.tmp / name
        writer = ParquetWriter(file, manifest.schema)
        for batch in manifest.to_batches():
            writer.write_batch(batch)
        writer.close()
        with open(file, "rb") as f:
            self.storage.upload_key_from_file(self._key(name), f, file.stat().st_size)

    def list_manifests(self) -> list[str]:
        return self.storage.list_key()

    def delete_manifest(self, name: str) -> None:
        self.storage.delete_key(self._key(name))

    def _key(self, name: str) -> str:
        return f"{self.node_id}/{name}"

    def copy(self) -> Self:
        return self.__class__(self.storage.copy(), self.node_id, self.tmp)
