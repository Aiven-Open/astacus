"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Test that the list endpoint behaves as advertised
"""

from astacus.common.ipc import (
    BackupManifest,
    ListForStorage,
    ListRequest,
    ListResponse,
    ListSingleBackup,
    Plugin,
    SnapshotFile,
    SnapshotHash,
    SnapshotResult,
    SnapshotState,
    SnapshotUploadResult,
)
from astacus.common.storage.manager import StorageManager
from astacus.coordinator import api
from astacus.coordinator.list import compute_deduplicated_snapshot_file_stats, list_backups
from fastapi.testclient import TestClient
from pytest_mock import MockerFixture

import datetime
import json
import pytest


def test_api_list(client: TestClient, populated_storage: StorageManager, mocker: MockerFixture) -> None:
    assert populated_storage

    def _run():
        response = client.get("/list")
        assert response.status_code == 200, response.json()

        assert response.json() == {
            "storages": [
                {
                    "backups": [
                        {
                            "attempt": 1,
                            "end": "2020-02-02T12:34:56+00:00",
                            "nodes": 1,
                            "files": 1,
                            "name": "1",
                            "plugin": "files",
                            "start": "2020-01-01T21:43:00+00:00",
                            "cluster_files": 1,
                            "cluster_data_size": 6,
                            "total_size": 6,
                            "upload_size": 6,
                            "upload_stored_size": 10,
                        },
                        {
                            "attempt": 1,
                            "end": "2020-02-02T12:34:56+00:00",
                            "nodes": 1,
                            "files": 1,
                            "name": "2",
                            "plugin": "files",
                            "start": "2020-01-01T21:43:00+00:00",
                            "cluster_files": 1,
                            "cluster_data_size": 6,
                            "total_size": 6,
                            "upload_size": 6,
                            "upload_stored_size": 10,
                        },
                    ],
                    "storage_name": "x",
                },
                {
                    "backups": [
                        {
                            "attempt": 1,
                            "end": "2020-02-02T12:34:56+00:00",
                            "nodes": 1,
                            "files": 1,
                            "name": "3",
                            "plugin": "files",
                            "start": "2020-01-01T21:43:00+00:00",
                            "cluster_files": 1,
                            "cluster_data_size": 6,
                            "total_size": 6,
                            "upload_size": 6,
                            "upload_stored_size": 10,
                        }
                    ],
                    "storage_name": "y",
                },
            ],
        }

    _run()

    # Second run should come from cache
    m = mocker.patch.object(api, "list_backups")
    _run()
    assert not m.called


@pytest.fixture(name="backup_manifest")
def fixture_backup_manifest() -> BackupManifest:
    """Provide a backup manifest with duplicate snapshot files.

    The test snapshot result has five unique snapshot files out of eight.
    The hexdigest is faked as the table UUID's last digit and a summary of the part name.
    """
    return BackupManifest(
        start=datetime.datetime(2020, 1, 2, 3, 4, 5, 678, tzinfo=datetime.timezone.utc),
        end=datetime.datetime(2020, 1, 2, 5, 6, 7, 891, tzinfo=datetime.timezone.utc),
        attempt=1,
        snapshot_results=[
            # First node
            SnapshotResult(
                state=SnapshotState(
                    root_globs=[],
                    files=[
                        # First table
                        SnapshotFile(
                            relative_path="store/000/00000000-0000-0000-0000-100000000001/detached/all_0_0_0/data.bin",
                            file_size=1000,
                            mtime_ns=0,
                            hexdigest="1000",
                        ),
                        SnapshotFile(
                            relative_path="store/000/00000000-0000-0000-0000-100000000001/detached/all_1_1_0/data.bin",
                            file_size=1000,
                            mtime_ns=0,
                            hexdigest="1110",
                        ),
                        SnapshotFile(
                            relative_path="store/000/00000000-0000-0000-0000-100000000001/detached/all_1_0_0/data.bin",
                            file_size=1000,
                            mtime_ns=0,
                            hexdigest="1100",
                        ),
                        # Second table
                        SnapshotFile(
                            relative_path="store/000/00000000-0000-0000-0000-100000000002/detached/all_0_0_0/data.bin",
                            file_size=1000,
                            mtime_ns=0,
                            hexdigest="2000",
                        ),
                    ],
                ),
                hashes=[
                    SnapshotHash(hexdigest="1000", size=1000),
                    SnapshotHash(hexdigest="1110", size=1000),
                    SnapshotHash(hexdigest="1100", size=1000),
                    SnapshotHash(hexdigest="2000", size=1000),
                ],
                files=4,
                total_size=4000,
            ),
            # Second node
            SnapshotResult(
                state=SnapshotState(
                    root_globs=[],
                    files=[
                        # First table
                        SnapshotFile(
                            relative_path="store/000/00000000-0000-0000-0000-100000000001/detached/all_0_0_0/data.bin",
                            file_size=1000,
                            mtime_ns=0,
                            hexdigest="1000",
                        ),
                        SnapshotFile(
                            relative_path="store/000/00000000-0000-0000-0000-100000000001/detached/all_1_1_0/data.bin",
                            file_size=1000,
                            mtime_ns=0,
                            hexdigest="1110",
                        ),
                        # Second table
                        SnapshotFile(
                            relative_path="store/000/00000000-0000-0000-0000-100000000002/detached/all_0_0_0/data.bin",
                            file_size=1000,
                            mtime_ns=0,
                            hexdigest="2000",
                        ),
                        SnapshotFile(
                            relative_path="store/000/00000000-0000-0000-0000-100000000002/detached/all_1_1_0/data.bin",
                            file_size=1000,
                            mtime_ns=0,
                            hexdigest="2110",
                        ),
                        # Third table with same hexdigest as second one
                        SnapshotFile(
                            relative_path="store/000/00000000-0000-0000-0000-100000000003/detached/all_0_0_0/data.bin",
                            file_size=1000,
                            mtime_ns=0,
                            hexdigest="2000",
                        ),
                    ],
                ),
                hashes=[
                    SnapshotHash(hexdigest="1000", size=1000),
                    SnapshotHash(hexdigest="1110", size=1000),
                    SnapshotHash(hexdigest="2000", size=1000),
                    SnapshotHash(hexdigest="2110", size=1000),
                    SnapshotHash(hexdigest="2000", size=1000),
                ],
                files=5,
                total_size=5000,
            ),
        ],
        upload_results=[
            SnapshotUploadResult(total_size=4000, total_stored_size=3000),
            SnapshotUploadResult(total_size=5000, total_stored_size=4000),
        ],
        plugin=Plugin.clickhouse,
    )


def test_compute_deduplicated_snapshot_file_stats(backup_manifest: BackupManifest) -> None:
    """Test backup stats are computed correctly in the presence of duplicate snapshot files."""
    manifest_json = json.loads(backup_manifest.json())
    snapshot_results_json = manifest_json["snapshot_results"]
    num_files, total_size = compute_deduplicated_snapshot_file_stats(snapshot_results_json)
    assert (num_files, total_size) == (6, 6000)


def test_api_list_deduplication(backup_manifest: BackupManifest, storage: StorageManager) -> None:
    """Test the list backup operation correctly deduplicates snapshot files when computing stats."""
    storage.get_json_store("x").upload_json("backup-1", backup_manifest)
    storage.get_hexdigest_store("x").upload_hexdigest_bytes("FAKEDIGEST", b"fake-digest-data")

    list_request = ListRequest(storage="x")
    list_response = list_backups(req=list_request, json_mstorage=storage.json_storage)
    expected_response = ListResponse(
        storages=[
            ListForStorage(
                storage_name="x",
                backups=[
                    ListSingleBackup(
                        name="1",
                        start=datetime.datetime(2020, 1, 2, 3, 4, 5, 678, tzinfo=datetime.timezone.utc),
                        end=datetime.datetime(2020, 1, 2, 5, 6, 7, 891, tzinfo=datetime.timezone.utc),
                        plugin=Plugin("clickhouse"),
                        attempt=1,
                        nodes=2,
                        cluster_files=6,
                        cluster_data_size=6000,
                        files=9,
                        total_size=9000,
                        upload_size=9000,
                        upload_stored_size=7000,
                    )
                ],
            ),
        ]
    )
    assert list_response == expected_response
