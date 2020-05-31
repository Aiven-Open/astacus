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

from .base import BackupOpBase, RestoreOpBase
from astacus.common import ipc
from astacus.common.utils import AstacusModel
from typing import List


class FilesConfiguration(AstacusModel):
    # list of globs, e.g. ["**/*.dat"] we want to back up from root
    root_globs: List[str]


class FilesBackupOp(BackupOpBase):
    steps = ["init"] + BackupOpBase.steps
    plugin = ipc.Plugin.files

    async def step_init(self):
        self.snapshot_root_globs = self.plugin_config.root_globs
        return True


class FilesRestoreOp(RestoreOpBase):
    plugin = ipc.Plugin.files


plugin_info = {"backup": FilesBackupOp, "restore": FilesRestoreOp, "config": FilesConfiguration}
