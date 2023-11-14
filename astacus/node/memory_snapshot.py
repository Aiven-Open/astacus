"""

Copyright (c) 2023 Aiven Ltd
See LICENSE for details

"""

from astacus.common import magic, utils
from astacus.common.ipc import SnapshotFile, SnapshotHash
from astacus.common.progress import increase_worth_reporting, Progress
from astacus.common.snapshot import SnapshotGroup
from astacus.node.snapshot import Snapshot
from astacus.node.snapshotter import hash_hexdigest_readable, Snapshotter
from pathlib import Path
from typing import Iterable, Iterator, Mapping, Sequence

import base64
import dataclasses
import logging
import os

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True, slots=True)
class FoundFile:
    relative_path: Path
    group: SnapshotGroup


class MemorySnapshot(Snapshot):
    def __init__(self, dst: Path) -> None:
        super().__init__(dst)
        self._relative_path_to_snapshotfile: dict[Path, SnapshotFile] = {}
        self._hexdigest_to_snapshotfiles: dict[str, list[SnapshotFile]] = {}

    def __len__(self) -> int:
        return len(self._relative_path_to_snapshotfile)

    def get_file(self, relative_path: Path) -> SnapshotFile | None:
        return self._relative_path_to_snapshotfile.get(relative_path, None)

    def get_files_for_digest(self, hexdigest: str) -> Iterable[SnapshotFile]:
        return self._hexdigest_to_snapshotfiles.get(hexdigest, [])

    def get_all_files(self) -> Iterable[SnapshotFile]:
        return self._relative_path_to_snapshotfile.values()

    def upsert_file(self, file: SnapshotFile) -> None:
        old_file = self.get_file(file.relative_path)
        if old_file:
            self.remove_file(old_file)
        self._relative_path_to_snapshotfile[file.relative_path] = file
        if file.hexdigest:
            self._hexdigest_to_snapshotfiles.setdefault(file.hexdigest, []).append(file)

    def remove_file(self, file: SnapshotFile) -> None:
        assert self._relative_path_to_snapshotfile[file.relative_path] == file
        del self._relative_path_to_snapshotfile[file.relative_path]
        if file.hexdigest:
            self._hexdigest_to_snapshotfiles[file.hexdigest].remove(file)

    def remove_path(self, relative_path: Path) -> None:
        file = self.get_file(relative_path)
        if file is not None:
            self.remove_file(file)

    def get_all_digests(self) -> Iterable[SnapshotHash]:
        return (
            SnapshotHash(hexdigest=file.hexdigest, size=file.file_size)
            for files in self._hexdigest_to_snapshotfiles.values()
            for file in files
        )


class MemorySnapshotter(Snapshotter[MemorySnapshot]):
    def _list_files(self, basepath: Path) -> list[FoundFile]:
        result_files = set()
        for group in self._groups.groups:
            for p in group.glob(root_dir=basepath):
                path = basepath / p
                if not path.is_file() or path.is_symlink():
                    continue
                relpath = path.relative_to(basepath)
                for parent in relpath.parents:
                    if parent.name == magic.ASTACUS_TMPDIR:
                        break
                else:
                    result_files.add(
                        FoundFile(
                            relative_path=relpath,
                            group=group.group.without_excluded_names(),
                        )
                    )
        return sorted(result_files, key=lambda found_file: found_file.relative_path)

    def _list_dirs_and_files(self, basepath: Path) -> tuple[list[Path], list[FoundFile]]:
        files = self._list_files(basepath)
        dirs = {p.relative_path.parent for p in files}
        return sorted(dirs), files

    def _snapshotfile_from_path(self, relative_path) -> SnapshotFile:
        src_path = self._src / relative_path
        st = src_path.stat()
        return SnapshotFile(relative_path=relative_path, mtime_ns=st.st_mtime_ns, file_size=st.st_size)

    def _get_snapshot_hash_list(self, found_files: Sequence[FoundFile]) -> Iterator[SnapshotFile]:
        same = 0
        lost = 0
        for found_file in found_files:
            old_snapshotfile = self.snapshot.get_file(found_file.relative_path)
            try:
                new_snapshotfile = self._snapshotfile_from_path(found_file.relative_path)
            except FileNotFoundError:
                lost += 1
                if increase_worth_reporting(lost):
                    logger.info(
                        "#%d. lost - %s disappeared before stat, ignoring", lost, self._src / found_file.relative_path
                    )
                continue
            if old_snapshotfile and old_snapshotfile.underlying_file_is_the_same(new_snapshotfile):
                new_snapshotfile.hexdigest = old_snapshotfile.hexdigest
                new_snapshotfile.content_b64 = old_snapshotfile.content_b64
                same += 1
                if increase_worth_reporting(same):
                    logger.info("#%d. same - %r in %s is same", same, old_snapshotfile, found_file.relative_path)
                continue

            self._maybe_link(found_file.relative_path)
            yield new_snapshotfile

    def _snapshot_create_missing_directories(self, *, src_dirs: Sequence[Path], dst_dirs: Sequence[Path]) -> int:
        changes = 0
        for i, relative_dir in enumerate(set(src_dirs).difference(dst_dirs), start=1):
            dst_path = self._dst / relative_dir
            dst_path.mkdir(parents=True, exist_ok=True)
            if increase_worth_reporting(i):
                logger.info("#%d. new directory: %r", i, relative_dir)
            changes += 1
        return changes

    def _snapshot_remove_extra_files(self, *, src_files: Sequence[FoundFile], dst_files: Sequence[FoundFile]) -> int:
        changes = 0
        for i, found_file in enumerate(set(dst_files).difference(src_files), start=1):
            dst_path = self._dst / found_file.relative_path
            snapshotfile = self.snapshot.get_file(found_file.relative_path)
            if snapshotfile:
                self.snapshot.remove_file(snapshotfile)
            dst_path.unlink(missing_ok=True)
            if increase_worth_reporting(i):
                logger.info("#%d. extra file: %r", i, found_file.relative_path)
            changes += 1
        return changes

    def _snapshot_add_missing_files(self, *, src_files: Sequence[FoundFile], dst_files: Sequence[FoundFile]) -> int:
        existing = 0
        disappeared = 0
        changes = 0
        for i, found_file in enumerate(set(src_files).difference(dst_files), start=1):
            src_path = self._src / found_file.relative_path
            dst_path = self._dst / found_file.relative_path
            try:
                os.link(src=src_path, dst=dst_path, follow_symlinks=False)
            except FileExistsError:
                # This happens only if snapshot is started twice at
                # same time. While it is technically speaking upstream
                # error, we rather handle it here than leave
                # exceptions not handled.
                existing += 1
                if increase_worth_reporting(existing):
                    logger.info("#%d. %s already existed, ignoring", existing, src_path)
                continue
            except FileNotFoundError:
                disappeared += 1
                if increase_worth_reporting(disappeared):
                    logger.info("#%d. %s disappeared before linking, ignoring", disappeared, src_path)
                continue
            if increase_worth_reporting(i - disappeared):
                logger.info("#%d. new file: %r", i - disappeared, found_file.relative_path)
            changes += 1
        return changes

    def perform_snapshot(self, *, progress: Progress) -> None:
        src_dirs, src_files = self._list_dirs_and_files(self._src)
        progress.start(1)
        changes = 0
        if self._src == self._dst:
            # The src=dst mode should be used if and only if it is
            # known that files will not disappear between snapshot and
            # upload steps (e.g. Astacus controls the lifecycle of the
            # files within). In that case, there is little point in
            # making extra symlinks and we can just use the src
            # directory contents as-is.
            dst_dirs, dst_files = src_dirs, src_files
            # When the src and dst files are identical, we can't compare the files of the previous
            # snapshot, but we still need to cleanup outdated files in relative_path_to_snapshotfile.
            relative_dst_files = {dst_file.relative_path for dst_file in dst_files}
            for outdated_relative_path in set(self.snapshot.get_all_paths()) - relative_dst_files:
                self.snapshot.remove_path(outdated_relative_path)
        else:
            progress.add_total(3)
            dst_dirs, dst_files = self._list_dirs_and_files(self._dst)

            # Create missing directories
            changes += self._snapshot_create_missing_directories(src_dirs=src_dirs, dst_dirs=dst_dirs)
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
            dst_dirs, dst_files = self._list_dirs_and_files(self._dst)

        snapshotfiles = list(self._get_snapshot_hash_list(dst_files))
        progress.add_total(len(snapshotfiles))
        path_to_group: Mapping[Path, SnapshotGroup] = {dst_file.relative_path: dst_file.group for dst_file in dst_files}

        def _cb(snapshotfile: SnapshotFile) -> SnapshotFile:
            # src may or may not be present; dst is present as it is in snapshot
            with snapshotfile.open_for_reading(self._dst) as f:
                group = path_to_group[snapshotfile.relative_path]
                if group.embedded_file_size_max is None or snapshotfile.file_size <= group.embedded_file_size_max:
                    snapshotfile.content_b64 = base64.b64encode(f.read()).decode()
                else:
                    snapshotfile.hexdigest = hash_hexdigest_readable(f)
            return snapshotfile

        def _result_cb(*, map_in: SnapshotFile, map_out: SnapshotFile) -> bool:
            self.snapshot.upsert_file(map_out)
            progress.add_success()
            return True

        changes += len(snapshotfiles)
        utils.parallel_map_to(iterable=snapshotfiles, fun=_cb, result_callback=_result_cb, n=self._parallel)

        # We initially started with 1 extra
        progress.add_success()

    def release(self, hexdigests: Iterable[str], *, progress: Progress) -> None:
        assert self._src != self._dst
        for hexdigest in progress.wrap(hexdigests):
            if hexdigest == "":
                continue

            for snapshotfile in self.snapshot.get_files_for_digest(hexdigest):
                self._release_file(snapshotfile)

    def _release_file(self, snapshotfile: SnapshotFile) -> None:
        file = self._dst / snapshotfile.relative_path
        file.unlink(missing_ok=True)
