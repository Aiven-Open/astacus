"""

Copyright (c) 2023 Aiven Ltd
See LICENSE for details

"""

from .node import NodeOp
from .snapshotter import Snapshotter
from .uploader import Uploader
from abc import ABC, abstractmethod
from astacus.common import ipc, utils
from astacus.common.ipc import SnapshotFile
from astacus.common.rohmustorage import RohmuStorage
from pathlib import Path
from typing import Iterable, Optional

import threading


class Snapshot(ABC):
    def __init__(self, dst: Path) -> None:
        self.lock = threading.Lock()
        self.dst = dst

    @abstractmethod
    def __len__(self) -> int:
        ...

    @abstractmethod
    def get_file(self, relative_path: Path) -> SnapshotFile | None:
        ...

    @abstractmethod
    def get_files_for_digest(self, hexdigest: str) -> Iterable[SnapshotFile]:
        ...

    @abstractmethod
    def get_all_files(self) -> Iterable[SnapshotFile]:
        ...

    def get_all_paths(self) -> Iterable[Path]:
        return (file.relative_path for file in self.get_all_files())

    def get_total_size(self) -> int:
        return sum(file.file_size for file in self.get_all_files())
