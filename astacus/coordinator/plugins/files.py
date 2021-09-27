"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

File backup plugin.

This is mostly implemented as sanity check, to ensure that the
building blocks mostly work as they should.

Configuration:
- root_globs

( which is also stored in the backup manifest, and used when restoring )

"""
from .base import (
    BackupManifestStep, BackupNameStep, CoordinatorPlugin, ListHexdigestsStep, OperationContext, RestoreStep, SnapshotStep,
    Step, UploadBlocksStep, UploadManifestStep
)
from astacus.common import ipc
from astacus.common.ipc import Plugin
from typing import List


class FilesPlugin(CoordinatorPlugin):
    # list of globs, e.g. ["**/*.dat"] we want to back up from root
    root_globs: List[str]

    def get_backup_steps(self, *, context: OperationContext) -> List[Step]:
        return [
            SnapshotStep(snapshot_root_globs=self.root_globs),
            ListHexdigestsStep(hexdigest_storage=context.hexdigest_storage),
            UploadBlocksStep(storage_name=context.storage_name),
            UploadManifestStep(json_storage=context.json_storage, plugin=Plugin.files)
        ]

    def get_restore_steps(self, *, context: OperationContext, req: ipc.RestoreRequest) -> List[Step]:
        return [
            BackupNameStep(json_storage=context.json_storage, requested_name=req.name),
            BackupManifestStep(json_storage=context.json_storage),
            RestoreStep(storage_name=context.storage_name, partial_restore_nodes=req.partial_restore_nodes)
        ]
