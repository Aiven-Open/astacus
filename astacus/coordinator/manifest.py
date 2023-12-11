"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from astacus.common import asyncstorage, ipc
from pathlib import Path
from starlette.concurrency import run_in_threadpool

import datetime


class BackupManifestFileWrapper:
    """Wrapper for large manifest files, caches some fields but always reads in
    whole file for `snapshot_results`.  This is needed because for the cleanup
    step we can have up to 30 large manifests, on the order of 500Mb each,
    so they can't all exist in memory at once.
    """

    def __init__(self, file_path: Path):
        self._file_path = file_path
        # filename is not set by ipc.BackupManifest
        self.filename = file_path.name
        self._is_cached = False

        self._start: datetime.datetime | None = None
        self._end: datetime.datetime | None = None
        self._attempt: int | None = None
        self._upload_results: list[ipc.SnapshotUploadResult] | None = None
        self._plugin: str | None = None
        self._plugin_data: dict | None = None

    def _load_and_cache_data(self):
        if not self._is_cached:
            manifest = ipc.BackupManifest.parse_file(self._file_path)
            self._start = manifest.start
            self._end = manifest.end
            self._attempt = manifest.attempt
            self._upload_results = manifest.upload_results
            self._plugin = manifest.plugin
            self._plugin_data = manifest.plugin_data
            self._is_cached = True

    @property
    def start(self) -> datetime.datetime:
        self._load_and_cache_data()
        assert self._start is not None
        return self._start

    @property
    def end(self) -> datetime.datetime:
        self._load_and_cache_data()
        assert self._end is not None
        return self._end

    @property
    def attempt(self) -> int:
        self._load_and_cache_data()
        assert self._attempt is not None
        return self._attempt

    @property
    def upload_results(self) -> list[ipc.SnapshotUploadResult]:
        self._load_and_cache_data()
        assert self._upload_results is not None
        return self._upload_results

    @property
    def plugin(self) -> str:
        self._load_and_cache_data()
        assert self._plugin is not None
        return self._plugin

    @property
    def plugin_data(self) -> dict:
        self._load_and_cache_data()
        assert self._plugin_data is not None
        return self._plugin_data

    def snapshot_results(self) -> list[ipc.SnapshotResult]:
        return ipc.BackupManifest.parse_file(self._file_path).snapshot_results

    def delete(self) -> None:
        return self._file_path.unlink()


async def download_backup_manifest_file_wrapper(
    json_storage: asyncstorage.AsyncJsonStorage, backup_name: str
) -> BackupManifestFileWrapper:
    return BackupManifestFileWrapper(await json_storage.download_json(backup_name))


async def download_backup_manifest(json_storage: asyncstorage.AsyncJsonStorage, backup_name: str) -> ipc.BackupManifest:
    d = await json_storage.download_and_read_json(backup_name)
    manifest = await run_in_threadpool(ipc.BackupManifest.parse_obj, d)
    assert not manifest.filename or manifest.filename == backup_name
    manifest.filename = backup_name
    return manifest
