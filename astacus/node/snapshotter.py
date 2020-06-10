"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

"""

from astacus.common import exceptions, magic, utils
from astacus.common.ipc import SnapshotFile, SnapshotHash, SnapshotState
from astacus.common.progress import Progress
from pathlib import Path
from typing import Optional

import base64
import hashlib
import logging
import os

logger = logging.getLogger(__name__)

_hash = hashlib.blake2s


def hash_hexdigest_readable(f, *, read_buffer=1_000_000):
    h = _hash()
    while True:
        data = f.read(read_buffer)
        if not data:
            break
        h.update(data)
    return h.hexdigest()


class Snapshotter:
    """Snapshotter keeps track of files on disk, and their hashes.

    The hash on disk MAY change, which may require subsequent
    incremential snapshot and-or ignoring the files which have changed.

    The output to outside is just root object's hash, as well as list
    of other hashes which correspond to files referred to within the
    file list contained in root object.

    For actual products, Snapshotter should be subclassed and
    e.g. file_path_filter should be overridden.
    """
    def __init__(self, *, src, dst, globs):
        assert globs  # model has empty; either plugin or configuration must supply them
        self.src = Path(src)
        self.dst = Path(dst)
        self.globs = globs
        self.relative_path_to_snapshotfile = {}
        self.hexdigest_to_snapshotfiles = {}

    def _list_files(self, basepath: Path):
        result_files = set()
        for glob in self.globs:
            for path in basepath.glob(glob):
                if not path.is_file() or path.is_symlink():
                    continue
                relpath = path.relative_to(basepath)
                for parent in relpath.parents:
                    if parent.name == magic.ASTACUS_TMPDIR:
                        break
                else:
                    result_files.add(relpath)
        return sorted(result_files)

    def _list_dirs_and_files(self, basepath: Path):
        files = self._list_files(basepath)
        dirs = {p.parent for p in files}
        return sorted(dirs), files

    def _add_snapshotfile(self, snapshotfile: SnapshotFile):
        old_snapshotfile = self.relative_path_to_snapshotfile.get(snapshotfile.relative_path, None)
        if old_snapshotfile:
            self._remove_snapshotfile(old_snapshotfile)
        self.relative_path_to_snapshotfile[snapshotfile.relative_path] = snapshotfile
        if snapshotfile.hexdigest:
            self.hexdigest_to_snapshotfiles.setdefault(snapshotfile.hexdigest, []).append(snapshotfile)

    def _remove_snapshotfile(self, snapshotfile: SnapshotFile):
        assert self.relative_path_to_snapshotfile[snapshotfile.relative_path] == snapshotfile
        del self.relative_path_to_snapshotfile[snapshotfile.relative_path]
        if snapshotfile.hexdigest:
            self.hexdigest_to_snapshotfiles[snapshotfile.hexdigest].remove(snapshotfile)

    def _snapshotfile_from_path(self, relative_path):
        src_path = self.src / relative_path
        st = src_path.stat()
        return SnapshotFile(relative_path=relative_path, mtime_ns=st.st_mtime_ns, file_size=st.st_size)

    def _get_snapshot_hash_list(self, relative_paths):
        for relative_path in relative_paths:
            old_snapshotfile = self.relative_path_to_snapshotfile.get(relative_path)
            try:
                snapshotfile = self._snapshotfile_from_path(relative_path)
            except FileNotFoundError:
                logger.debug("%s disappeared before stat, ignoring", self.src / relative_path)

            if old_snapshotfile:
                snapshotfile.hexdigest = old_snapshotfile.hexdigest
                snapshotfile.content_b64 = old_snapshotfile.content_b64
                if old_snapshotfile == snapshotfile:
                    logger.debug("%r in %s is same", old_snapshotfile, relative_path)
                    continue
            yield snapshotfile

    def get_snapshot_hashes(self):
        return [
            SnapshotHash(hexdigest=dig, size=sf[0].file_size) for dig, sf in self.hexdigest_to_snapshotfiles.items() if sf
        ]

    def get_snapshot_state(self):
        return SnapshotState(root_globs=self.globs, files=sorted(self.relative_path_to_snapshotfile.values()))

    def snapshot(self, *, progress: Optional[Progress] = None):
        if progress is None:
            progress = Progress()
        progress.start(3)

        src_dirs, src_files = self._list_dirs_and_files(self.src)
        dst_dirs, dst_files = self._list_dirs_and_files(self.dst)

        # Create missing directories
        changes = 0
        for relative_dir in set(src_dirs).difference(dst_dirs):
            dst_path = self.dst / relative_dir
            dst_path.mkdir(parents=True, exist_ok=True)
            changes += 1

        progress.add_success()

        # Remove extra files
        for relative_path in set(dst_files).difference(src_files):
            dst_path = self.dst / relative_path
            snapshotfile = self.relative_path_to_snapshotfile.get(relative_path)
            if snapshotfile:
                self._remove_snapshotfile(snapshotfile)
            dst_path.unlink()
            changes += 1
        progress.add_success()

        # Add missing files
        for relative_path in set(src_files).difference(dst_files):
            src_path = self.src / relative_path
            dst_path = self.dst / relative_path
            try:
                os.link(src=src_path, dst=dst_path, follow_symlinks=False)
                changes += 1
            except FileNotFoundError:
                logger.debug("%s disappeared before linking, ignoring", src_path)
        progress.add_success()

        # We COULD also remove extra directories, but it is not
        # probably really worth it and due to ignored files it
        # actually might not even work.

        # Then, create/update corresponding snapshotfile objects (old
        # ones were already removed)
        snapshotfiles = list(self._get_snapshot_hash_list(src_files))
        progress.add_total(len(snapshotfiles))
        # TBD: This could be done via e.g. multiprocessing too some day.
        for snapshotfile in snapshotfiles:
            # src may or may not be present; dst is present as it is in snapshot
            if snapshotfile.file_size <= magic.EMBEDDED_FILE_SIZE:
                snapshotfile.content_b64 = base64.b64encode(snapshotfile.open_for_reading(self.dst).read()).decode()
            else:
                snapshotfile.hexdigest = hash_hexdigest_readable(snapshotfile.open_for_reading(self.dst))
            self._add_snapshotfile(snapshotfile)
            changes += 1
            progress.add_success()
        progress.done()
        return changes

    def write_hashes_to_storage(self, *, hashes, storage, parallel, progress: Progress, still_running_callback=lambda: True):
        todo = set(hash.hexdigest for hash in hashes)
        progress.start(len(todo))
        sizes = {"total": 0, "stored": 0}

        def _download_hexdigest_in_thread(hexdigest):
            assert hexdigest
            files = self.hexdigest_to_snapshotfiles.get(hexdigest, [])
            for snapshotfile in files:
                path = self.dst / snapshotfile.relative_path
                if not path.is_file():
                    logger.warning("%s disappeared post-snapshot", path)
                    continue
                current_hexdigest = hash_hexdigest_readable(snapshotfile.open_for_reading(self.dst))
                if current_hexdigest != snapshotfile.hexdigest:
                    logger.info("Hash of %s changed before upload", snapshotfile.relative_path)
                    continue
                try:
                    upload_result = storage.upload_hexdigest_from_file(hexdigest, snapshotfile.open_for_reading(self.dst))
                except exceptions.TransientException:
                    # We found and processed one file with the particular
                    # hexdigest; even if sending it failed, we won't try
                    # subsequent files and instead break out of iterating
                    # through candidate files with same hexdigest.
                    return progress.upload_failure, 0, 0
                current_hexdigest = hash_hexdigest_readable(snapshotfile.open_for_reading(self.dst))
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

        sorted_todo = sorted(todo, key=lambda hexdigest: -self.hexdigest_to_snapshotfiles[hexdigest][0].file_size)
        if not utils.parallel_map_to(
            fun=_download_hexdigest_in_thread, iterable=sorted_todo, result_callback=_result_cb, n=parallel
        ):
            progress.add_fail()

        # This operation is done. It may or may not have been a success.
        progress.done()
        return sizes["total"], sizes["stored"]
