"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

"""

from astacus.common import magic
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
