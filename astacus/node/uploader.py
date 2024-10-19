"""Copyright (c) 2020 Aiven Ltd
See LICENSE for details.

"""

from .snapshotter import hash_hexdigest_readable
from astacus.common import exceptions, utils
from astacus.common.ipc import SnapshotFile, SnapshotHash
from astacus.common.progress import Progress
from astacus.common.storage import ThreadLocalStorage
from astacus.node.snapshot import Snapshot
from collections.abc import Sequence

import dataclasses
import logging

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True, kw_only=True)
class Uploader:
    thread_local_storage: ThreadLocalStorage

    def write_hashes_to_storage(
        self,
        *,
        snapshot: Snapshot,
        hashes: Sequence[SnapshotHash],
        parallel: int,
        progress: Progress,
        still_running_callback=lambda: True,
        validate_file_hashes: bool = True,
    ):
        todo = [
            (hexdigest, list(snapshot.get_files_for_digest(hexdigest)))
            for hexdigest in set(hash.hexdigest for hash in hashes)
        ]
        todo.sort(key=lambda hexdigest_and_files: -hexdigest_and_files[1][0].file_size)
        progress.start(len(todo))
        sizes = {"total": 0, "stored": 0}

        def _upload_hexdigest_in_thread(work: tuple[str, list[SnapshotFile]]):
            hexdigest, files = work
            storage = self.thread_local_storage.get_storage()

            assert hexdigest
            files = list(snapshot.get_files_for_digest(hexdigest))
            for snapshotfile in files:
                path = snapshot.dst / snapshotfile.relative_path
                if not path.is_file():
                    logger.warning("%s disappeared post-snapshot", path)
                    continue
                if validate_file_hashes:
                    with snapshotfile.open_for_reading(snapshot.dst) as f:
                        current_hexdigest = hash_hexdigest_readable(f)
                    if current_hexdigest != snapshotfile.hexdigest:
                        logger.info("Hash of %s changed before upload", snapshotfile.relative_path)
                        continue
                try:
                    with snapshotfile.open_for_reading(snapshot.dst) as f:
                        upload_result = storage.upload_hexdigest_from_file(hexdigest, f, file_size=snapshotfile.file_size)
                except exceptions.TransientException as ex:
                    # Do not pollute logs with transient exceptions
                    logger.info("Transient exception uploading %r: %r", path, ex)
                    return progress.upload_failure, 0, 0
                except exceptions.AstacusException:
                    # Report failure - whole step will be retried later
                    logger.exception("Exception uploading %r", path)
                    return progress.upload_failure, 0, 0
                if validate_file_hashes:
                    with snapshotfile.open_for_reading(snapshot.dst) as f:
                        current_hexdigest = hash_hexdigest_readable(f)
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

        if not utils.parallel_map_to(fun=_upload_hexdigest_in_thread, iterable=todo, result_callback=_result_cb, n=parallel):
            progress.add_fail()
        return sizes["total"], sizes["stored"]
