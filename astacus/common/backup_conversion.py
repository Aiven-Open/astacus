from astacus.common import ipc
from astacus.common.node_manifest import ARROW_SCHEMA, convert_pydantic_to_row
from astacus.common.storage.file import FileStorage
from astacus.common.storage.json import JsonStore
from astacus.common.storage.node import NodeManifestStore
from pathlib import Path
from typing import TypedDict

import base64
import pyarrow
import pyarrow.dataset as ds
import sys
import tempfile


def convert_backup(backup: ipc.BackupManifest) -> tuple[ipc.BackupManifestV20240225, list[ds.Dataset]]:
    converted_snapshot_results = []
    datasets = []
    for idx, result in enumerate(backup.snapshot_results):
        assert result.state is not None
        converted_result = ipc.SnapshotResultV20240225(
            start=result.start,
            end=result.end,
            files=result.files,
            total_size=result.total_size,
            root_globs=result.state.root_globs,
            node_id=idx,
        )
        converted_snapshot_results.append(converted_result)
        pylist = list(map(convert_pydantic_to_row, result.state.files))
        table = pyarrow.Table.from_pylist(pylist, schema=ARROW_SCHEMA)
        datasets.append(ds.dataset(table))

    converted_backup = ipc.BackupManifestV20240225(
        start=backup.start,
        end=backup.end,
        plugin=backup.plugin,
        attempt=backup.attempt,
        snapshot_results=converted_snapshot_results,
        upload_results=backup.upload_results,
        filename=backup.filename,
    )
    return converted_backup, datasets


def main() -> None:
    file = sys.argv[1]
    folder = Path(sys.argv[2])
    backup = ipc.BackupManifest.parse_file(file)
    converted_backup, datasets = convert_backup(backup)
    storage = FileStorage(folder)
    json_store = JsonStore(storage)
    json_store.upload_json("backup-new.json", converted_backup)
    with tempfile.TemporaryDirectory() as tmp:
        for i, dataset in enumerate(datasets):
            store = NodeManifestStore(FileStorage(folder), i, Path(tmp))
            store.upload_manifest("backup-new.parquet", dataset)


if __name__ == "__main__":
    main()
