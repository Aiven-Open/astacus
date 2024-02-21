"""

Copyright (c) 2023 Aiven Ltd
See LICENSE for details

"""

from abc import ABC, abstractmethod
from astacus.common.ipc import SnapshotFile, SnapshotHash
from pathlib import Path
from typing import Iterable

import threading

SnapshotFileTuple = tuple[str, int, int, str, str | None]


class Snapshot(ABC):
    def __init__(self, dst: Path) -> None:
        self.lock = threading.Lock()
        self.dst = dst

    @abstractmethod
    def __len__(self) -> int: ...

    @abstractmethod
    def get_file(self, relative_path: str) -> SnapshotFile | None: ...

    @abstractmethod
    def get_files_for_digest(self, hexdigest: str) -> Iterable[SnapshotFile]: ...

    def get_all_files(self) -> Iterable[SnapshotFile]:
        return map(tuple_to_snapshotfile, self.get_all_files_tuples())

    @abstractmethod
    def get_all_files_tuples(self) -> Iterable[SnapshotFileTuple]: ...

    def get_all_paths(self) -> Iterable[str]:
        return (file.relative_path for file in self.get_all_files())

    @abstractmethod
    def get_all_digests(self) -> Iterable[SnapshotHash]: ...

    def get_total_size(self) -> int:
        return sum(file.file_size for file in self.get_all_files())


def tuple_to_snapshotfile(row: SnapshotFileTuple) -> SnapshotFile:
    return SnapshotFile(relative_path=row[0], file_size=row[1], mtime_ns=row[2], hexdigest=row[3], content_b64=row[4])
