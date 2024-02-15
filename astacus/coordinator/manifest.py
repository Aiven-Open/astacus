"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""

from astacus.common import ipc
from astacus.common.storage.asyncio import AsyncJsonStore
from starlette.concurrency import run_in_threadpool


async def download_backup_manifest(json_storage: AsyncJsonStore, backup_name: str) -> ipc.BackupManifest:
    d = await json_storage.download_json(backup_name)
    manifest = await run_in_threadpool(ipc.BackupManifest.parse_obj, d)
    assert not manifest.filename or manifest.filename == backup_name
    manifest.filename = backup_name
    return manifest
