"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from astacus.common import asyncstorage, ipc
from starlette.concurrency import run_in_threadpool

import msgspec


async def download_backup_manifest(json_storage: asyncstorage.AsyncJsonStorage, backup_name: str) -> ipc.BackupManifest:
    def download_manifest() -> ipc.BackupManifest:
        with json_storage.storage.open_json_bytes(backup_name) as manifest_content:
            return msgspec.json.decode(manifest_content, type=ipc.BackupManifest)

    manifest = await run_in_threadpool(download_manifest)
    assert not manifest.filename or manifest.filename == backup_name
    manifest.filename = backup_name
    return manifest


async def download_backup_min_manifest(json_storage: asyncstorage.AsyncJsonStorage, backup_name: str) -> ipc.ManifestMin:
    def download_min_manifest() -> ipc.ManifestMin:
        with json_storage.storage.open_json_bytes(backup_name) as manifest_content:
            return msgspec.json.decode(manifest_content, type=ipc.ManifestMin)

    manifest = await run_in_threadpool(download_min_manifest)
    assert not manifest.filename or manifest.filename == backup_name
    manifest.filename = backup_name
    return manifest
