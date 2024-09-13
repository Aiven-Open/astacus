"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""

from astacus.common import asyncstorage, ipc
from astacus.common.storage import JsonStorage
from starlette.concurrency import run_in_threadpool


def download_backup_manifest_sync(json_storage: JsonStorage, backup_name: str) -> ipc.BackupManifest:
    manifest = json_storage.download_json(backup_name, ipc.BackupManifest)
    assert not manifest.filename or manifest.filename == backup_name
    manifest.filename = backup_name
    return manifest


async def download_backup_manifest(json_storage: asyncstorage.AsyncJsonStorage, backup_name: str) -> ipc.BackupManifest:
    return await run_in_threadpool(download_backup_manifest_sync, json_storage.storage, backup_name)


async def download_backup_min_manifest(json_storage: asyncstorage.AsyncJsonStorage, backup_name: str) -> ipc.ManifestMin:
    def download_min_manifest() -> ipc.ManifestMin:
        return json_storage.storage.download_json(backup_name, ipc.ManifestMin)

    manifest = await run_in_threadpool(download_min_manifest)
    assert not manifest.filename or manifest.filename == backup_name
    manifest.filename = backup_name
    return manifest
