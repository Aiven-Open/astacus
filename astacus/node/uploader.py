"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

"""

from .snapshotter import hash_hexdigest_readable, Snapshotter
from astacus.common import exceptions, utils
from astacus.common.progress import Progress
from astacus.common.storage import ThreadLocalStorage

import logging

logger = logging.getLogger(__name__)


class Uploader(ThreadLocalStorage):
    def write_hashes_to_storage(
        self, *, snapshotter: Snapshotter, hashes, parallel: int, progress: Progress, still_running_callback=lambda: True
    ):
        todo = set(hash.hexdigest for hash in hashes)
        progress.start(len(todo))
        sizes = {"total": 0, "stored": 0}

        def _download_hexdigest_in_thread(hexdigest):
            storage = self.local_storage

            assert hexdigest
            files = snapshotter.hexdigest_to_snapshotfiles.get(hexdigest, [])
            for snapshotfile in files:
                path = snapshotter.dst / snapshotfile.relative_path
                if not path.is_file():
                    logger.warning("%s disappeared post-snapshot", path)
                    continue
                current_hexdigest = hash_hexdigest_readable(snapshotfile.open_for_reading(snapshotter.dst))
                if current_hexdigest != snapshotfile.hexdigest:
                    logger.info("Hash of %s changed before upload", snapshotfile.relative_path)
                    continue
                try:
                    upload_result = storage.upload_hexdigest_from_file(
                        hexdigest, snapshotfile.open_for_reading(snapshotter.dst)
                    )
                except exceptions.TransientException:
                    # We found and processed one file with the particular
                    # hexdigest; even if sending it failed, we won't try
                    # subsequent files and instead break out of iterating
                    # through candidate files with same hexdigest.
                    return progress.upload_failure, 0, 0
                current_hexdigest = hash_hexdigest_readable(snapshotfile.open_for_reading(snapshotter.dst))
                if current_hexdigest != snapshotfile.hexdigest:
                    logger.info("Hash of %s changed after upload", snapshotfile.relative_path)
                    storage.delete_hexdigest(hexdigest)
                    continue
                return progress.upload_success, upload_result.size, upload_result.stored_size

            # We didn't find single file with the matching hexdigest.
            # Report it as missing but keep uploading other files.
            return progress.upload_missing, 0, 0

        def _result_cb(*, map_in, map_out):
            # progress callback in 'main' thread
            progress_callback, total, stored = map_out
            sizes["total"] += total
            sizes["stored"] += stored
            progress_callback(map_in)  # hexdigest
            return still_running_callback()

        sorted_todo = sorted(todo, key=lambda hexdigest: -snapshotter.hexdigest_to_snapshotfiles[hexdigest][0].file_size)
        if not utils.parallel_map_to(
            fun=_download_hexdigest_in_thread, iterable=sorted_todo, result_callback=_result_cb, n=parallel
        ):
            progress.add_fail()
        return sizes["total"], sizes["stored"]
