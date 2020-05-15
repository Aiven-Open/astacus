"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from .progress import Progress
from pathlib import Path
from pydantic import BaseModel  # pylint: disable=no-name-in-module # ( sometimes Cython -> pylint won't work )
from typing import List, Optional

import functools

# node generic base


class NodeRequest(BaseModel):
    result_url: str = ""  # where results are sent


class NodeResult(BaseModel):
    progress: Progress


# node.snapshot


@functools.total_ordering
class SnapshotFile(BaseModel):
    relative_path: Path
    file_size: int
    mtime_ns: int
    hexdigest: str

    def __lt__(self, o):
        # In our use case, paths uniquely identify files we care about
        return self.relative_path < o.relative_path


class SnapshotState(BaseModel):
    files: List[SnapshotFile]


class SnapshotRequest(NodeRequest):
    pass


class SnapshotUploadRequest(NodeRequest):
    hashes: List[str]


class SnapshotResult(NodeResult):
    state: Optional[SnapshotState]  # should be passed opaquely to restore
    hashes: Optional[List[str]]  # populated only if state is available


class SnapshotUploadResult(NodeResult):
    pass