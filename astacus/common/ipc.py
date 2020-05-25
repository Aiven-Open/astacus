"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from .progress import Progress
from .utils import AstacusModel
from datetime import datetime
from pathlib import Path
from pydantic import Field
from typing import List, Optional

import functools
import socket

# node generic base


class NodeRequest(AstacusModel):
    result_url: str = ""  # where results are sent


class NodeResult(AstacusModel):
    hostname: str = Field(default_factory=socket.gethostname)
    progress: Progress


# node.snapshot


@functools.total_ordering
class SnapshotFile(AstacusModel):
    relative_path: Path
    file_size: int
    mtime_ns: int
    hexdigest: str

    def __lt__(self, o):
        # In our use case, paths uniquely identify files we care about
        return self.relative_path < o.relative_path


class SnapshotState(AstacusModel):
    files: List[SnapshotFile]


class SnapshotRequest(NodeRequest):
    pass


class SnapshotHash(AstacusModel):
    """
    This class represents something that is to be stored in the object storage.

    size is provided mainly to allow for even loading of nodes in case
    same hexdigest is available from multiple nodes.

    For symmetry, same structure is passed back in SnapshotUploadRequest,
    although only hexdigest should really matter.
    """
    hexdigest: str
    size: int

    def __hash__(self):
        # hexdigests should be unique, regardless of size
        return hash(self.hexdigest)


class SnapshotUploadRequest(NodeRequest):
    hashes: List[SnapshotHash]


class SnapshotResult(NodeResult):
    start: datetime = Field(default_factory=datetime.now)
    end: Optional[datetime]
    state: Optional[SnapshotState]  # should be passed opaquely to restore
    hashes: Optional[List[SnapshotHash]]  # populated only if state is available


class SnapshotUploadResult(NodeResult):
    pass


# controller.backup


class BackupManifest(AstacusModel):
    attempt: int
    snapshot_results: List[SnapshotResult]
