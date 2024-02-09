"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

"""
from astacus.common import ipc, magic
from astacus.common.storage import JsonStorage, MultiStorage
from collections import defaultdict
from collections.abc import Iterator

import msgspec


def compute_deduplicated_snapshot_file_stats(manifest: ipc.BackupManifest) -> tuple[int, int]:
    """Compute stats over snapshot files as identified by their hex digest.

    There may be duplicate hex digests within nodes for multiple copies of the same data chunks.
    Duplicates' size are aggregated as they require physical disk space during restore.
    On the other hand, hex digests can be safely deduplicated across nodes. The ocurrence with
    the highest associated file count is kept, giving an upper bound for the cluster data size.
    """
    hexdigest_max_counts: dict[str, int] = {}
    hexdigest_sizes: dict[str, int] = {}
    for snapshot_result in manifest.snapshot_results:
        assert snapshot_result.state is not None
        node_hexdigest_counter: defaultdict[str, int] = defaultdict(lambda: 0)
        for snapshot_file in snapshot_result.state.files:
            node_hexdigest_counter[snapshot_file.hexdigest] += 1
            if snapshot_file.hexdigest not in hexdigest_sizes:
                hexdigest_sizes[snapshot_file.hexdigest] = snapshot_file.file_size
        for hexdigest, count in node_hexdigest_counter.items():
            max_count = hexdigest_max_counts.get(hexdigest, 0)
            hexdigest_max_counts[hexdigest] = max(max_count, count)
    num_files = sum(hexdigest_max_counts.values())
    total_size = sum(count * hexdigest_sizes[hexdigest] for hexdigest, count in hexdigest_max_counts.items())
    return num_files, total_size


def _iter_backups(storage: JsonStorage, backup_prefix: str) -> Iterator[ipc.ListSingleBackup]:
    for name in sorted(storage.list_jsons()):
        if not name.startswith(backup_prefix):
            continue
        pname = name[len(backup_prefix) :]
        with storage.open_json_bytes(name) as manifest_content:
            manifest = msgspec.json.decode(manifest_content, type=ipc.BackupManifest)
        files = sum(x.files for x in manifest.snapshot_results)
        total_size = sum(x.total_size for x in manifest.snapshot_results)
        upload_size = sum(x.total_size for x in manifest.upload_results)
        upload_stored_size = sum(x.total_stored_size for x in manifest.upload_results)
        cluster_files, cluster_data_size = compute_deduplicated_snapshot_file_stats(manifest)
        yield ipc.ListSingleBackup(
            name=pname,
            start=manifest.start,
            end=manifest.end,
            plugin=manifest.plugin,
            attempt=manifest.attempt,
            nodes=len(manifest.snapshot_results),
            files=files,
            cluster_files=cluster_files,
            total_size=total_size,
            cluster_data_size=cluster_data_size,
            upload_size=upload_size,
            upload_stored_size=upload_stored_size,
        )


def _iter_storages(
    req: ipc.ListRequest, json_mstorage: MultiStorage, backup_prefix: str = magic.JSON_BACKUP_PREFIX
) -> Iterator[ipc.ListForStorage]:
    # req.storage is optional, used to constrain listing just to the
    # given storage. by default, we list all storages.
    for storage_name in sorted(json_mstorage.list_storages()):
        if not req.storage or req.storage == storage_name:
            backups = list(_iter_backups(json_mstorage.get_storage(storage_name), backup_prefix=backup_prefix))
            yield ipc.ListForStorage(storage_name=storage_name, backups=backups)


def list_backups(*, req: ipc.ListRequest, json_mstorage: MultiStorage) -> ipc.ListResponse:
    return ipc.ListResponse(storages=list(_iter_storages(req, json_mstorage)))


def list_delta_backups(*, req: ipc.ListRequest, json_mstorage: MultiStorage) -> ipc.ListResponse:
    return ipc.ListResponse(storages=list(_iter_storages(req, json_mstorage, backup_prefix=magic.JSON_DELTA_PREFIX)))
