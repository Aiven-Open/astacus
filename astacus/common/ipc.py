"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from .progress import Progress
from .utils import AstacusModel
from datetime import datetime
from enum import Enum
from pathlib import Path
from pydantic import Field
from typing import List, Optional

import functools
import socket


# These are the database plugins we support; list is intentionally
# enum here, as dynamically adding them isn't priority (for now)
class Plugin(str, Enum):
    files = "files"


# node generic base


class NodeRequest(AstacusModel):
    result_url: str = ""  # where results are sent


class NodeResult(AstacusModel):
    hostname: str = Field(default_factory=socket.gethostname)
    az: str = ""
    progress: Progress = Field(default_factory=Progress)


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

    def equals_excluding_mtime(self, o):
        return self.copy(update={"mtime_ns": 0}) == o.copy(update={"mtime_ns": 0})


class SnapshotState(AstacusModel):
    root_globs: List[str]
    files: List[SnapshotFile]


class SnapshotRequest(NodeRequest):
    # list of globs, e.g. ["**/*.dat"] we want to back up from root
    root_globs: List[str]


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


class SnapshotDownloadRequest(NodeRequest):
    state: SnapshotState


# controller.backup


class BackupManifest(AstacusModel):
    # When was (this) backup attempt started
    start: datetime

    # .. and when did it finish
    end: datetime = Field(default_factory=datetime.now)

    # How many attempts did it take (starts from 1)
    attempt: int

    # Filesystem snapshot contents of the backup
    snapshot_results: List[SnapshotResult]

    # Which plugin was used to back the data up
    plugin: Plugin

    # Plugin-specific data about the backup
    plugin_data: dict = {}
