"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

"""

from astacus.common import ipc, magic
from astacus.common.storage import MultiStorage


def _iter_backups(storage):
    for name in sorted(storage.list_jsons()):
        if not name.startswith(magic.JSON_BACKUP_PREFIX):
            continue
        pname = name[len(magic.JSON_BACKUP_PREFIX):]
        manifest = ipc.BackupManifest.parse_obj(storage.download_json(name))
        files = sum(x.files for x in manifest.snapshot_results)
        total_size = sum(x.total_size for x in manifest.snapshot_results)
        upload_size = sum(x.total_size for x in manifest.upload_results)
        upload_stored_size = sum(x.total_stored_size for x in manifest.upload_results)
        yield ipc.ListSingleBackup(
            name=pname,
            start=manifest.start,
            end=manifest.end,
            plugin=manifest.plugin,
            attempt=manifest.attempt,
            files=files,
            total_size=total_size,
            upload_size=upload_size,
            upload_stored_size=upload_stored_size,
        )


def _iter_storages(req, json_mstorage):
    # req.storage is optional, used to constrain listing just to the
    # given storage. by default, we list all storages.
    for storage_name in sorted(json_mstorage.list_storages()):
        if not req.storage or req.storage == storage_name:
            backups = list(_iter_backups(json_mstorage.get_storage(storage_name)))
            yield ipc.ListForStorage(storage_name=storage_name, backups=backups)


def list_backups(*, req: ipc.ListRequest, json_mstorage: MultiStorage) -> ipc.ListResponse:
    return ipc.ListResponse(storages=list(_iter_storages(req, json_mstorage)))
