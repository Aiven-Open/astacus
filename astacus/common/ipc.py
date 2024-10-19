"""Copyright (c) 2020 Aiven Ltd
See LICENSE for details.
"""

from .magic import DEFAULT_EMBEDDED_FILE_SIZE, StrEnum
from .progress import Progress
from .utils import now, SizeLimitedFile
from astacus.common.snapshot import SnapshotGroup
from collections.abc import Sequence, Set
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Self

import functools
import msgspec
import socket

# pydantic validators are class methods in disguise
# pylint: disable=no-self-argument


# These are the database plugins we support; list is intentionally
# enum here, as dynamically adding them isn't priority (for now)
class Plugin(StrEnum):
    cassandra = "cassandra"
    clickhouse = "clickhouse"
    files = "files"
    m3db = "m3db"
    flink = "flink"


class NodeFeatures(Enum):
    # Added on 2022-11-29, this can be assumed to be supported everywhere after 1 or 2 years
    validate_file_hashes = "validate_file_hashes"
    # Added on 2023-06-07
    snapshot_groups = "snapshot_groups"
    # Added on 2023-10-16
    release_snapshot_files = "release_snapshot_files"


class Retention(msgspec.Struct, kw_only=True):
    # If set, number of backups to retain always (even beyond days)
    minimum_backups: int | None = None

    # If set, maximum number of backups to retain
    maximum_backups: int | None = None

    # Backups older than this are deleted, unless it would reduce
    # number of backups to less than minimum_backups
    keep_days: int | None = None


# node generic base


class NodeRequest(msgspec.Struct, kw_only=True):
    result_url: str = ""  # where results are sent


class NodeResult(msgspec.Struct, kw_only=True):
    hostname: str = msgspec.field(default_factory=socket.gethostname)
    az: str = ""
    progress: Progress = msgspec.field(default_factory=Progress)


class MetadataResult(msgspec.Struct, kw_only=True):
    version: str
    features: Sequence[str] = msgspec.field(default_factory=list)


# node.snapshot


@functools.total_ordering
class SnapshotFile(msgspec.Struct, kw_only=True, omit_defaults=True):
    relative_path: str
    file_size: int
    mtime_ns: int
    hexdigest: str = ""
    content_b64: str | None = None

    def __lt__(self, o: "SnapshotFile") -> bool:
        # In our use case, paths uniquely identify files we care about
        return self.relative_path < o.relative_path

    def underlying_file_is_the_same(self, o: "SnapshotFile") -> bool:
        return self.mtime_ns == o.mtime_ns and self.relative_path == o.relative_path and self.file_size == o.file_size

    def equals_excluding_mtime(self, o: "SnapshotFile") -> bool:
        return (self.relative_path, self.file_size, self.hexdigest, self.content_b64) == (
            o.relative_path,
            o.file_size,
            o.hexdigest,
            o.content_b64,
        )

    def open_for_reading(self, root_path: Path) -> SizeLimitedFile:
        return SizeLimitedFile(path=root_path / self.relative_path, file_size=self.file_size)


class SnapshotState(msgspec.Struct, kw_only=True, omit_defaults=True):
    root_globs: Sequence[str] = msgspec.field(default_factory=list)
    files: Sequence[SnapshotFile] = msgspec.field(default_factory=list)


class SnapshotRequest(NodeRequest):
    # list of globs, e.g. ["**/*.dat"] we want to back up from root
    root_globs: Sequence[str] = ()


class SnapshotRequestGroup(msgspec.Struct, kw_only=True):
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


def create_snapshot_request(
    snapshot_groups: Sequence[SnapshotGroup], *, node_features: Set[str]
) -> SnapshotRequestV2 | SnapshotRequest:
    if NodeFeatures.snapshot_groups.value in node_features:
        return SnapshotRequestV2(
            groups=[
                SnapshotRequestGroup(
                    root_glob=group.root_glob,
                    excluded_names=group.excluded_names,
                    embedded_file_size_max=group.embedded_file_size_max,
                )
                for group in snapshot_groups
            ],
        )
    # This is a lossy backward compatibility since the extra options are not passed
    return SnapshotRequest(
        root_globs=[group.root_glob for group in snapshot_groups],
    )


class SnapshotHash(msgspec.Struct, kw_only=True, frozen=True):
    """This class represents something that is to be stored in the object storage.

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
    hashes: Sequence[SnapshotHash]

    # which (sub)object storage entry should be used
    storage: str


# Added on 2022-11-29, the previous version should be removable after 1 or 2 years.
class SnapshotUploadRequestV20221129(SnapshotUploadRequest):
    # Whether we should check if the file hash has changed since the snapshot
    # Should be False only for plugins where the database is known to never
    # change the files that we are reading: for instance because the database
    # itself created a snapshot and Astacus is reading from that snapshot.
    validate_file_hashes: bool = True


class SnapshotUploadResult(NodeResult, kw_only=True):
    total_size: int = 0
    total_stored_size: int = 0


class SnapshotResult(NodeResult):
    # when was the operation started ( / done )
    start: datetime = msgspec.field(default_factory=now)
    end: datetime | None = None

    # The state is optional because it's written by the Snapshotter post-initialization.
    # If the backup failed, the related manifest doesn't exist: the state and the
    # summary attributes below will be set to none and their default values respectively.
    # Should be passed opaquely to restore.
    state: SnapshotState | None = msgspec.field(default_factory=SnapshotState)

    # Summary data for manifest use
    files: int = 0
    total_size: int = 0

    # populated only if state is available
    hashes: Sequence[SnapshotHash] | None = None


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
    tokens: Sequence[str] | None = None
    replace_address_first_boot: str | None = None
    skip_bootstrap_streaming: bool | None = None


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
class PartialRestoreRequestNode(msgspec.Struct, kw_only=True):
    def __post_init__(self) -> None:
        if (self.backup_index is None) == (self.backup_hostname is None):
            raise ValueError("Exactly one of backup_index or backup_hostname supported")
        if (self.node_index is None) == (self.node_url is None):
            raise ValueError("Exactly one of node_index or node_url supported")

    # One of these has to be specified
    #
    # index = index in configuration
    # hostname = hostname of the host that did the backup
    backup_index: int | None = None
    backup_hostname: str | None = None

    # One of these has to be specified
    #
    # index = index in configuration
    # url = URL of the Astacus endpoint for the particular node
    node_index: int | None = None
    node_url: str | None = None


class RestoreRequest(msgspec.Struct, kw_only=True):
    storage: str = ""
    name: str = ""
    partial_restore_nodes: Sequence[PartialRestoreRequestNode] | None = None
    stop_after_step: str | None = None


class TieredStorageResults(msgspec.Struct, kw_only=True):
    n_objects: int
    total_size_bytes: int


# coordinator.plugins backup/restore
class BackupManifest(msgspec.Struct, kw_only=True):
    # When was (this) backup attempt started
    start: datetime

    # .. and when did it finish
    end: datetime = msgspec.field(default_factory=now)

    # How many attempts did it take (starts from 1)
    attempt: int

    # Filesystem snapshot contents of the backup
    snapshot_results: Sequence[SnapshotResult]

    # What did the upload return (mostly for statistics)
    upload_results: Sequence[SnapshotUploadResult]

    # Which plugin was used to back the data up
    plugin: Plugin

    # Plugin-specific data about the backup
    plugin_data: dict = msgspec.field(default_factory=dict)

    # Semi-redundant but simplifies handling; automatically set on download
    filename: str = ""

    tiered_storage_results: TieredStorageResults | None = None


class ManifestMin(msgspec.Struct, kw_only=True):
    start: datetime
    end: datetime
    filename: str

    @classmethod
    def from_manifest(cls, manifest: BackupManifest) -> Self:
        return cls(start=manifest.start, end=manifest.end, filename=manifest.filename)


# coordinator.list


class ListRequest(msgspec.Struct, kw_only=True):
    storage: str = ""


class ListSingleBackup(msgspec.Struct, frozen=True, kw_only=True):
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
    # Total size in bytes of files in tiered storage
    tiered_storage_size: int | None = None


class ListForStorage(msgspec.Struct, kw_only=True):
    storage_name: str
    backups: Sequence[ListSingleBackup]


class ListResponse(msgspec.Struct, kw_only=True):
    storages: Sequence[ListForStorage]


# coordinator.cleanup


class CleanupRequest(msgspec.Struct, kw_only=True):
    storage: str = ""
    retention: Retention | None = None
    explicit_delete: Sequence[str] = msgspec.field(default_factory=list)
