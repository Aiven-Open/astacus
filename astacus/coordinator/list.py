"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

"""

from astacus.common import ipc, magic
from astacus.common.json_view import (
    get_array,
    get_int,
    get_optional_object,
    get_optional_str,
    get_str,
    iter_objects,
    JsonArrayView,
)
from astacus.common.storage.base import MultiStorage
from astacus.common.storage.file import FileMultiStorage
from astacus.common.storage.manager import StorageManager
from astacus.common.utils import now
from collections import defaultdict
from collections.abc import Iterator
from pathlib import Path
from pydantic.datetime_parse import parse_datetime
from tempfile import mkdtemp

import pyarrow
import pyarrow.compute as pc
import pyarrow.dataset as ds


def compute_deduplicated_snapshot_file_stats_v1(snapshot_results_json: JsonArrayView) -> tuple[int, int]:
    """Compute stats over snapshot files as identified by their hex digest.

    There may be duplicate hex digests within nodes for multiple copies of the same data chunks.
    Duplicates' size are aggregated as they require physical disk space during restore.
    On the other hand, hex digests can be safely deduplicated across nodes. The ocurrence with
    the highest associated file count is kept, giving an upper bound for the cluster data size.
    """
    hexdigest_max_counts: dict[str, int] = {}
    hexdigest_sizes: dict[str, int] = {}
    for snapshot_result in iter_objects(snapshot_results_json):
        state = get_optional_object(snapshot_result, "state")
        assert state is not None
        node_hexdigest_counter: defaultdict[str, int] = defaultdict(lambda: 0)
        for snapshot_file in iter_objects(get_array(state, "files")):
            hexdigest = get_str(snapshot_file, "hexdigest", "")
            file_size = get_int(snapshot_file, "file_size")
            node_hexdigest_counter[hexdigest] += 1
            if hexdigest not in hexdigest_sizes:
                hexdigest_sizes[hexdigest] = file_size
        for hexdigest, count in node_hexdigest_counter.items():
            max_count = hexdigest_max_counts.get(hexdigest, 0)
            hexdigest_max_counts[hexdigest] = max(max_count, count)
    num_files = sum(hexdigest_max_counts.values())
    total_size = sum(count * hexdigest_sizes[hexdigest] for hexdigest, count in hexdigest_max_counts.items())
    return num_files, total_size


def compute_deduplicated_snapshot_file_stats_v2(
    manifest: ipc.BackupManifestV20240225, node_datasets: ds.Dataset
) -> tuple[int, int]:
    tables = []
    num_files = 0
    total_size = 0
    for dataset in node_datasets:
        table = dataset.to_table(columns=["hexdigest", "file_size"])
        embedded_files = table.filter(pc.is_null(pc.field("hexdigest"))).column("file_size")
        # embedded files
        if len(embedded_files):
            num_files += len(embedded_files)
            total_size += pc.sum(embedded_files).as_py()
        # handle hashed files
        hashsed_files = (
            table.filter(pc.is_valid(pc.field("hexdigest")))
            .group_by(["hexdigest"])
            .aggregate([("file_size", "count"), ("file_size", "max")])
        )
        tables.append(hashsed_files)
    result = (
        pyarrow.concat_tables(tables)
        .group_by(["hexdigest"])
        .aggregate([("file_size_count", "max"), ("file_size_max", "max")])
    )
    total_size += pc.sum(pc.multiply(result.column("file_size_count_max"), result.column("file_size_max_max"))).as_py()
    num_files += pc.sum(result.column("file_size_count_max")).as_py()
    return num_files, total_size


def _iter_backups(storage_name: str | None, storage: StorageManager, backup_prefix: str) -> Iterator[ipc.ListSingleBackup]:
    json_store = storage.get_json_store(storage_name)
    for name in sorted(json_store.list_jsons()):
        if not name.startswith(backup_prefix):
            continue
        pname = name[len(backup_prefix) :]
        manifest_json = json_store.download_json(name)
        assert isinstance(manifest_json, dict)
        if "version" not in manifest_json:
            yield compute_backup_list_result_v1(pname, manifest_json)
        else:
            manifest = ipc.BackupManifestV20240225(**manifest_json)
            node_stores = storage.get_node_stores(storage_name)
            node_datasets = [store.download_manifest(parquet_key_from_json_key(name)) for store in node_stores]
            yield compute_backup_list_result_v2(pname, manifest, node_datasets)


def compute_backup_list_result_v1(name: str, manifest_json: dict) -> ipc.ListSingleBackup:
    snapshot_results_json = get_array(manifest_json, "snapshot_results")
    upload_results_json = get_array(manifest_json, "upload_results")
    files = sum(get_int(x, "files", 0) for x in iter_objects(snapshot_results_json))
    total_size = sum(get_int(x, "total_size", 0) for x in iter_objects(snapshot_results_json))
    upload_size = sum(get_int(x, "total_size", 0) for x in iter_objects(upload_results_json))
    upload_stored_size = sum(get_int(x, "total_stored_size", 0) for x in iter_objects(upload_results_json))
    cluster_files, cluster_data_size = compute_deduplicated_snapshot_file_stats_v1(snapshot_results_json)
    start = parse_datetime(get_str(manifest_json, "start"))
    maybe_end = get_optional_str(manifest_json, "end", None)
    # The default value is odd, but that's what is defined in `ipc.BackupManifest`.
    end = parse_datetime(maybe_end) if maybe_end is not None else now()
    return ipc.ListSingleBackup(
        name=name,
        start=start,
        end=end,
        plugin=ipc.Plugin(get_str(manifest_json, "plugin")),
        attempt=get_int(manifest_json, "attempt"),
        nodes=len(snapshot_results_json),
        files=files,
        cluster_files=cluster_files,
        total_size=total_size,
        cluster_data_size=cluster_data_size,
        upload_size=upload_size,
        upload_stored_size=upload_stored_size,
    )


def compute_backup_list_result_v2(
    name: str, manifest: ipc.BackupManifestV20240225, node_datasets: list[ds.Dataset]
) -> ipc.ListSingleBackup:
    cluster_files, cluster_data_size = compute_deduplicated_snapshot_file_stats_v2(manifest, node_datasets)
    return ipc.ListSingleBackup(
        name=name,
        start=manifest.start,
        end=manifest.end,
        plugin=manifest.plugin,
        attempt=manifest.attempt,
        nodes=len(node_datasets),
        files=sum(result.files for result in manifest.snapshot_results),
        total_size=sum(result.total_size for result in manifest.snapshot_results),
        cluster_files=cluster_files,
        cluster_data_size=cluster_data_size,
        upload_size=sum(result.total_size for result in manifest.upload_results),
        upload_stored_size=sum(result.total_stored_size for result in manifest.upload_results),
    )


def _iter_storages(
    req: ipc.ListRequest, storage: StorageManager, backup_prefix: str = magic.JSON_BACKUP_PREFIX
) -> Iterator[ipc.ListForStorage]:
    # req.storage is optional, used to constrain listing just to the
    # given storage. by default, we list all storages.
    for storage_name in sorted(storage.json_storage.list_storages()):
        if not req.storage or req.storage == storage_name:
            backups = list(_iter_backups(req.storage, storage, backup_prefix=backup_prefix))
            yield ipc.ListForStorage(storage_name=storage_name, backups=backups)


def list_backups(*, req: ipc.ListRequest, storage: StorageManager) -> ipc.ListResponse:
    return ipc.ListResponse(storages=list(_iter_storages(req, storage)))


def list_delta_backups(*, req: ipc.ListRequest, json_mstorage: MultiStorage) -> ipc.ListResponse:
    return ipc.ListResponse(storages=list(_iter_storages(req, json_mstorage, backup_prefix=magic.JSON_DELTA_PREFIX)))


def parquet_key_from_json_key(json_key: str) -> str:
    if not json_key.endswith(".json"):
        return json_key
    return json_key.removesuffix(".json") + ".parquet"


def main() -> None:
    path = "testing/old"
    storage = FileMultiStorage(path)
    manager = StorageManager(
        n_nodes=1,
        default_storage_name="",
        json_storage=storage,
        hexdigest_storage=storage,
        node_storage=storage,
        tmp_path=Path(mkdtemp()),
    )

    for res in _iter_backups(None, storage=manager, backup_prefix=magic.JSON_BACKUP_PREFIX):
        print(res)


if __name__ == "__main__":
    main()
