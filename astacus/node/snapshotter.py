"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

"""

from abc import ABC, abstractmethod
from astacus.common.ipc import SnapshotFile, SnapshotState
from astacus.common.progress import Progress
from astacus.common.snapshot import SnapshotGroup
from astacus.node.snapshot import Snapshot
from multiprocessing import dummy
from pathlib import Path
from threading import Lock
from typing import Generic, Iterable, Sequence, TypeVar

import base64
import hashlib
import os

T = TypeVar("T", bound=Snapshot)


class Snapshotter(ABC, Generic[T]):
    def __init__(self, groups: Sequence[SnapshotGroup], src: Path, dst: Path, snapshot: T, parallel: int) -> None:
        assert groups  # model has empty; either plugin or configuration must supply them
        self.snapshot = snapshot
        self._src = src
        self._dst = dst
        self._groups = groups
        self._parallel = parallel
        self._dst.mkdir(parents=True, exist_ok=True)

    @property
    def lock(self) -> Lock:
        return self.snapshot.lock

    @abstractmethod
    def perform_snapshot(self, *, progress: Progress) -> None:
        ...

    def get_snapshot_state(self) -> SnapshotState:
        return SnapshotState(
            root_globs=[group.root_glob for group in self._groups], files=list(self.snapshot.get_all_files())
        )

    def _file_in_src(self, relative_path: Path) -> SnapshotFile:
        src_path = self._src / relative_path
        st = src_path.stat()
        return SnapshotFile(relative_path=relative_path, mtime_ns=st.st_mtime_ns, file_size=st.st_size)

    def _compute_digests(self, files: Iterable[SnapshotFile]) -> Iterable[SnapshotFile]:
        def _cb(snapshotfile: SnapshotFile) -> SnapshotFile:
            # src may or may not be present; dst is present as it is in snapshot
            with snapshotfile.open_for_reading(self._dst) as f:
                embedded_file_size_max = self._embedded_file_size_max_for_file(snapshotfile)
                if embedded_file_size_max is None or snapshotfile.file_size <= embedded_file_size_max:
                    if snapshotfile.content_b64 is None:
                        snapshotfile.content_b64 = base64.b64encode(f.read()).decode()
                else:
                    if snapshotfile.hexdigest == "":
                        snapshotfile.hexdigest = hash_hexdigest_readable(f)
            return snapshotfile

        with dummy.Pool(self._parallel) as p:
            yield from p.imap_unordered(_cb, files)

    def _embedded_file_size_max_for_file(self, file: SnapshotFile) -> int | None:
        groups = []
        for group in self._groups:
            if file.relative_path.match(group.root_glob):
                groups.append(group)
        assert groups
        head, *tail = groups
        for group in tail:
            if not group.embedded_file_size_max == head.embedded_file_size_max:
                raise ValueError("All SnapshotGroups containing a common file must have the same embedded_file_size_max")
        return head.embedded_file_size_max

    def _maybe_link(self, relpath: Path) -> None:
        """Links the src to the dst if we are not in same root mode.
        If dst exists, it is unlinked first.
        """
        if self._same_root_mode():
            return

        src = self._src / relpath
        dst = self._dst / relpath
        dst.unlink(missing_ok=True)
        os.link(src=src, dst=dst, follow_symlinks=False)

    def _same_root_mode(self) -> bool:
        return self._src.samefile(self._dst)


_hash = hashlib.blake2s


def hash_hexdigest_readable(f, *, read_buffer: int = 1_000_000) -> str:
    h = _hash()
    while True:
        data = f.read(read_buffer)
        if not data:
            break
        h.update(data)
    return h.hexdigest()
