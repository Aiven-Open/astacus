"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

"""

from astacus.common import magic, utils
from astacus.common.ipc import SnapshotFile, SnapshotHash, SnapshotState
from astacus.common.progress import increase_worth_reporting, Progress
from pathlib import Path
from typing import Optional

import base64
import hashlib
import logging
import os
import threading

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

    Note that any call to public API MUST be made with
    snapshotter.lock held. This is because Snapshotter is process-wide
    utility that is shared across operations, possibly used from
    multiple threads, and the single-operation-only mode of operation
    is not exactly flawless (the 'new operation can be started with
    old running' is intentional feature but new operation should
    eventually replace the old). The lock itself might not need to be
    built-in to Snapshotter, but having it there enables asserting its
    state during public API calls.
    """
    def __init__(self, *, src, dst, globs, parallel):
        assert globs  # model has empty; either plugin or configuration must supply them
        self.src = Path(src)
        self.dst = Path(dst)
        self.globs = globs
        self.relative_path_to_snapshotfile = {}
        self.hexdigest_to_snapshotfiles = {}
        self.parallel = parallel
        self.lock = threading.Lock()

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
        same = 0
        lost = 0
        for relative_path in relative_paths:
            old_snapshotfile = self.relative_path_to_snapshotfile.get(relative_path)
            try:
                snapshotfile = self._snapshotfile_from_path(relative_path)
            except FileNotFoundError:
                lost += 1
                if increase_worth_reporting(lost):
                    logger.debug("#%d. lost - %s disappeared before stat, ignoring", lost, self.src / relative_path)
                continue
            if old_snapshotfile:
                snapshotfile.hexdigest = old_snapshotfile.hexdigest
                snapshotfile.content_b64 = old_snapshotfile.content_b64
                if old_snapshotfile == snapshotfile:
                    same += 1
                    if increase_worth_reporting(same):
                        logger.debug("#%d. same - %r in %s is same", same, old_snapshotfile, relative_path)
                    continue
            yield snapshotfile

    def get_snapshot_hashes(self):
        assert self.lock.locked()
        return [
            SnapshotHash(hexdigest=dig, size=sf[0].file_size) for dig, sf in self.hexdigest_to_snapshotfiles.items() if sf
        ]

    def get_snapshot_state(self):
        assert self.lock.locked()
        return SnapshotState(root_globs=self.globs, files=sorted(self.relative_path_to_snapshotfile.values()))

    def _snapshot_create_missing_directories(self, *, src_dirs, dst_dirs):
        changes = 0
        for i, relative_dir in enumerate(set(src_dirs).difference(dst_dirs), 1):
            dst_path = self.dst / relative_dir
            dst_path.mkdir(parents=True, exist_ok=True)
            if increase_worth_reporting(i):
                logger.debug("#%d. new directory: %r", i, relative_dir)
            changes += 1
        return changes

    def _snapshot_remove_extra_files(self, *, src_files, dst_files):
        changes = 0
        for i, relative_path in enumerate(set(dst_files).difference(src_files), 1):
            dst_path = self.dst / relative_path
            snapshotfile = self.relative_path_to_snapshotfile.get(relative_path)
            if snapshotfile:
                self._remove_snapshotfile(snapshotfile)
            dst_path.unlink()
            if increase_worth_reporting(i):
                logger.debug("#%d. extra file: %r", i, relative_path)
            changes += 1
        return changes

    def _snapshot_add_missing_files(self, *, src_files, dst_files):
        existing = 0
        disappeared = 0
        changes = 0
        for i, relative_path in enumerate(set(src_files).difference(dst_files), 1):
            src_path = self.src / relative_path
            dst_path = self.dst / relative_path
            try:
                os.link(src=src_path, dst=dst_path, follow_symlinks=False)
            except FileExistsError:
                # This happens only if snapshot is started twice at
                # same time. While it is technically speaking upstream
                # error, we rather handle it here than leave
                # exceptions not handled.
                existing += 1
                if increase_worth_reporting(existing):
                    logger.debug("#%d. %s already existed, ignoring", existing, src_path)
                continue
            except FileNotFoundError:
                disappeared += 1
                if increase_worth_reporting(disappeared):
                    logger.debug("#%d. %s disappeared before linking, ignoring", disappeared, src_path)
                continue
            if increase_worth_reporting(i - disappeared):
                logger.debug("#%d. new file: %r", i - disappeared, relative_path)
            changes += 1
        return changes

    def snapshot(self, *, progress: Optional[Progress] = None):
        assert self.lock.locked()

        if progress is None:
            progress = Progress()
        progress.start(3)

        src_dirs, src_files = self._list_dirs_and_files(self.src)
        dst_dirs, dst_files = self._list_dirs_and_files(self.dst)

        # Create missing directories
        changes = self._snapshot_create_missing_directories(src_dirs=src_dirs, dst_dirs=dst_dirs)
        progress.add_success()

        # Remove extra files
        changes += self._snapshot_remove_extra_files(src_files=src_files, dst_files=dst_files)
        progress.add_success()

        # Add missing files
        changes += self._snapshot_add_missing_files(src_files=src_files, dst_files=dst_files)
        progress.add_success()

        # We COULD also remove extra directories, but it is not
        # probably really worth it and due to ignored files it
        # actually might not even work.

        # Then, create/update corresponding snapshotfile objects (old
        # ones were already removed)
        dst_dirs, dst_files = self._list_dirs_and_files(self.dst)
        snapshotfiles = list(self._get_snapshot_hash_list(dst_files))
        progress.add_total(len(snapshotfiles))

        def _cb(snapshotfile):
            # src may or may not be present; dst is present as it is in snapshot
            with snapshotfile.open_for_reading(self.dst) as f:
                if snapshotfile.file_size <= magic.EMBEDDED_FILE_SIZE:
                    snapshotfile.content_b64 = base64.b64encode(f.read()).decode()
                else:
                    snapshotfile.hexdigest = hash_hexdigest_readable(f)
            return snapshotfile

        def _result_cb(*, map_in, map_out):
            self._add_snapshotfile(map_out)
            progress.add_success()
            return True

        changes += len(snapshotfiles)
        utils.parallel_map_to(iterable=snapshotfiles, fun=_cb, result_callback=_result_cb, n=self.parallel)
        return changes
