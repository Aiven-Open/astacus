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
from astacus.common.storage import JsonStorage, MultiStorage
from astacus.common.utils import now
from collections import defaultdict
from collections.abc import Iterator
from pydantic.datetime_parse import parse_datetime


def compute_deduplicated_snapshot_file_stats(snapshot_results_json: JsonArrayView) -> tuple[int, int]:
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


def _iter_backups(storage: JsonStorage, backup_prefix: str) -> Iterator[ipc.ListSingleBackup]:
    for name in sorted(storage.list_jsons()):
        if not name.startswith(backup_prefix):
            continue
        pname = name[len(backup_prefix) :]
        manifest_json = storage.download_json(name)
        assert isinstance(manifest_json, dict)
        snapshot_results_json = get_array(manifest_json, "snapshot_results")
        upload_results_json = get_array(manifest_json, "upload_results")
        files = sum(get_int(x, "files", 0) for x in iter_objects(snapshot_results_json))
        total_size = sum(get_int(x, "total_size", 0) for x in iter_objects(snapshot_results_json))
        upload_size = sum(get_int(x, "total_size", 0) for x in iter_objects(upload_results_json))
        upload_stored_size = sum(get_int(x, "total_stored_size", 0) for x in iter_objects(upload_results_json))
        cluster_files, cluster_data_size = compute_deduplicated_snapshot_file_stats(snapshot_results_json)
        start = parse_datetime(get_str(manifest_json, "start"))
        maybe_end = get_optional_str(manifest_json, "end", None)
        # The default value is odd, but that's what is defined in `ipc.BackupManifest`.
        end = parse_datetime(maybe_end) if maybe_end is not None else now()
        yield ipc.ListSingleBackup(
            name=pname,
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
