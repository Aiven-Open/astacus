"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

General restore utilities that are product independent.

The basic file restoration steps should be implementable by using the
API of this module with proper parameters.

"""

from .snapshot import SnapshotFile, SnapshotState
from typing import Dict, List

import os
import shutil


class Restorer:
    def __init__(self, *, dst):
        self.dst = dst

    def restore_from_storage(self, *, storage, progress, snapshotstate: SnapshotState, still_running_callback=lambda: True):
        hexdigest_to_snapshotfiles: Dict[str, List[SnapshotFile]] = {}
        for snapshotfile in snapshotstate.files:
            hexdigest_to_snapshotfiles.setdefault(snapshotfile.hexdigest, []).append(snapshotfile)
        progress.start(sum(snapshotfiles[0].file_size for snapshotfiles in hexdigest_to_snapshotfiles.values()))
        # TBD: Error checking, what to do if we're told to restore to existing directory?
        for hexdigest, snapshotfiles in hexdigest_to_snapshotfiles.items():
            if not still_running_callback():
                break
            download_path = self.dst / snapshotfiles[0].relative_path
            download_path.parent.mkdir(parents=True, exist_ok=True)
            with download_path.open("wb") as f:
                storage.download_hexdigest_to_file(hexdigest, f)
                progress.download_success(snapshotfiles[0].file_size)
                os.utime(download_path, ns=(snapshotfiles[0].mtime_ns, snapshotfiles[0].mtime_ns))

            # We don't report progress for these, as local copying
            # should be ~instant
            for snapshotfile in snapshotfiles[1:]:
                copy_path = self.dst / snapshotfile.relative_path
                copy_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(download_path, copy_path)
                os.utime(copy_path, ns=(snapshotfile.mtime_ns, snapshotfile.mtime_ns))
        # This operation is done. It may or may not have been a success.
        progress.done()
