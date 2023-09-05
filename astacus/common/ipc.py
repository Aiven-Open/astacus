"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

# pydantic validators are class methods in disguise
# pylint: disable=no-self-argument

from .magic import DEFAULT_EMBEDDED_FILE_SIZE, StrEnum
from .progress import Progress
from .utils import AstacusModel, now, SizeLimitedFile
from datetime import datetime
from pathlib import Path
from pydantic import Field, root_validator
from typing import List, Optional, Sequence

import functools
import socket


# These are the database plugins we support; list is intentionally
# enum here, as dynamically adding them isn't priority (for now)
class Plugin(StrEnum):
    cassandra = "cassandra"
    clickhouse = "clickhouse"
    files = "files"
    m3db = "m3db"
    flink = "flink"


class Retention(AstacusModel):
    # If set, number of backups to retain always (even beyond days)
    minimum_backups: Optional[int] = None

    # If set, maximum number of backups to retain
    maximum_backups: Optional[int] = None

    # Backups older than this are deleted, unless it would reduce
    # number of backups to less than minimum_backups
    keep_days: Optional[int] = None


# node generic base


class NodeRequest(AstacusModel):
    result_url: str = ""  # where results are sent


class NodeResult(AstacusModel):
    hostname: str = Field(default_factory=socket.gethostname)
    az: str = ""
    progress: Progress = Field(default_factory=Progress)


class MetadataResult(AstacusModel):
    version: str
    features: Sequence[str] = Field(default=list)  # type: ignore


# node.snapshot


@functools.total_ordering
class SnapshotFile(AstacusModel):
    relative_path: Path
    file_size: int
    mtime_ns: int
    hexdigest: str = ""
    content_b64: Optional[str]

    def __lt__(self, o: "SnapshotFile") -> bool:
        # In our use case, paths uniquely identify files we care about
        return self.relative_path < o.relative_path

    def underlying_file_is_the_same(self, o: "SnapshotFile") -> bool:
        return self.mtime_ns == o.mtime_ns and self.relative_path == o.relative_path and self.file_size == o.file_size

    def equals_excluding_mtime(self, o: "SnapshotFile") -> bool:
        return self.copy(update={"mtime_ns": 0}) == o.copy(update={"mtime_ns": 0})

    def open_for_reading(self, root_path: Path) -> SizeLimitedFile:
        return SizeLimitedFile(path=root_path / self.relative_path, file_size=self.file_size)


class SnapshotState(AstacusModel):
    root_globs: Sequence[str] = []
    files: Sequence[SnapshotFile] = []


class SnapshotRequest(NodeRequest):
    # list of globs, e.g. ["**/*.dat"] we want to back up from root
    root_globs: Sequence[str] = ()


class SnapshotRequestGroup(AstacusModel):
    root_glob: str
    # Exclude some file names that matched the glob
    excluded_names: Sequence[str] = ()
    # None means "no limit": all files matching the glob will be embedded
    embedded_file_size_max: int | None = DEFAULT_EMBEDDED_FILE_SIZE


class SnapshotRequestV2(NodeRequest):
    # list of globs with extra options for each glob.
    groups: Sequence[SnapshotRequestGroup] = ()
    # Accept V1 request for backward compatibility if the controller is older
    root_globs: Sequence[str] = ()


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
    # list of hashes to be uploaded
    hashes: List[SnapshotHash]

    # which (sub)object storage entry should be used
    storage: str


# Added on 2022-11-29, the previous version should be removable after 1 or 2 years.
class SnapshotUploadRequestV20221129(SnapshotUploadRequest):
    # Whether we should check if the file hash has changed since the snapshot
    # Should be False only for plugins where the database is known to never
    # change the files that we are reading: for instance because the database
    # itself created a snapshot and Astacus is reading from that snapshot.
    validate_file_hashes: bool = True


class SnapshotUploadResult(NodeResult):
    total_size: int = 0
    total_stored_size: int = 0


class SnapshotResult(NodeResult):
    # when was the operation started ( / done )
    start: datetime = Field(default_factory=now)
    end: Optional[datetime]

    # The state is optional because it's written by the Snapshotter post-initialization.
    # If the backup failed, the related manifest doesn't exist: the state and the
    # summary attributes below will be set to none and their default values respectively.
    # Should be passed opaquely to restore.
    state: Optional[SnapshotState] = Field(default_factory=SnapshotState)

    # Summary data for manifest use
    files: int = 0
    total_size: int = 0

    # populated only if state is available
    hashes: Optional[List[SnapshotHash]]


class SnapshotDownloadRequest(NodeRequest):
    # which (sub)object storage entry should be used
    storage: str

    # which backup
    backup_name: str

    # which snapshot within the backup
    snapshot_index: int

    # this is used to configure snapshotter; it is needed in the main
    # thread of node, so due to that, it is included here and not
    # retrieved via backup manifest.
    root_globs: Sequence[str]


class SnapshotClearRequest(NodeRequest):
    # Files not matching this are not deleted
    root_globs: Sequence[str]


class SnapshotReleaseRequest(NodeRequest):
    # Files matching these digests will be unlinked in snapshotter's dst
    hexdigests: Sequence[str]


# node.cassandra


class CassandraSubOp(StrEnum):
    get_schema_hash = "get-schema-hash"
    remove_snapshot = "remove-snapshot"
    unrestore_sstables = "unrestore-sstables"
    restore_sstables = "restore-sstables"
    start_cassandra = "start-cassandra"
    stop_cassandra = "stop-cassandra"
    take_snapshot = "take-snapshot"


class CassandraStartRequest(NodeRequest):
    tokens: Optional[List[str]]
    replace_address_first_boot: Optional[str]
    skip_bootstrap_streaming: Optional[bool]


class CassandraGetSchemaHashResult(NodeResult):
    schema_hash: str


class CassandraTableMatching(StrEnum):
    cfid = "cfid"
    cfname = "cfname"


class CassandraRestoreSSTablesRequest(NodeRequest):
    table_glob: str
    keyspaces_to_skip: Sequence[str]
    match_tables_by: CassandraTableMatching
    expect_empty_target: bool


# coordinator.api
class PartialRestoreRequestNode(AstacusModel):
    # One of these has to be specified
    #
    # index = index in configuration
    # hostname = hostname of the host that did the backup
    backup_index: Optional[int]
    backup_hostname: Optional[str]

    @root_validator
    def _check_only_one_backup_criteria(cls, values):
        if (values["backup_index"] is None) == (values["backup_hostname"] is None):
            raise ValueError("Exactly one of backup_index or backup_hostname supported")
        return values

    # One of these has to be specified
    #
    # index = index in configuration
    # url = URL of the Astacus endpoint for the particular node
    node_index: Optional[int]
    node_url: Optional[str]

    @root_validator
    def _check_only_one_node_criteria(cls, values):
        if (values["node_index"] is None) == (values["node_url"] is None):
            raise ValueError("Exactly one of node_index or node_url supported")
        return values


class RestoreRequest(AstacusModel):
    storage: str = ""
    name: str = ""
    partial_restore_nodes: Optional[List[PartialRestoreRequestNode]]
    stop_after_step: Optional[str] = None


# coordinator.plugins backup/restore
class BackupManifest(AstacusModel):
    # When was (this) backup attempt started
    start: datetime

    # .. and when did it finish
    end: datetime = Field(default_factory=now)

    # How many attempts did it take (starts from 1)
    attempt: int

    # Filesystem snapshot contents of the backup
    snapshot_results: List[SnapshotResult]

    # What did the upload return (mostly for statistics)
    upload_results: List[SnapshotUploadResult]

    # Which plugin was used to back the data up
    plugin: Plugin

    # Plugin-specific data about the backup
    plugin_data: dict = {}

    # Semi-redundant but simplifies handling; automatically set on download
    filename: str = ""


# coordinator.list


class ListRequest(AstacusModel):
    storage: str = ""


class ListSingleBackup(AstacusModel):
    # Subset of BackupManifest; see it for information
    name: str
    start: datetime
    end: datetime
    plugin: Plugin
    attempt: int
    nodes: int
    files: int
    total_size: int
    upload_size: int
    upload_stored_size: int
    # Number of cluster files and total cluster data size
    # The two fields are computed from deduplicated files across nodes but not within nodes,
    # using the files' hexdigest as key. As such, they are *not* sourced from BackupManifest.
    cluster_files: int
    cluster_data_size: int


class ListForStorage(AstacusModel):
    storage_name: str
    backups: List[ListSingleBackup]


class ListResponse(AstacusModel):
    storages: List[ListForStorage]


# coordinator.cleanup


class CleanupRequest(AstacusModel):
    storage: str = ""
    retention: Optional[Retention] = None
    explicit_delete: List[str] = []
