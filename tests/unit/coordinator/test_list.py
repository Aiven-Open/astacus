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
from astacus.common.rohmustorage import MultiRohmuStorage
from astacus.coordinator import api
from astacus.coordinator.api import get_cache_entries_from_list_response
from astacus.coordinator.list import compute_deduplicated_snapshot_file_stats, list_backups
from fastapi.testclient import TestClient
from pathlib import Path
from pytest_mock import MockerFixture
from tests.utils import create_rohmu_config
from unittest import mock

import datetime
import pytest


def test_api_list(client: TestClient, populated_mstorage: MultiRohmuStorage, mocker: MockerFixture) -> None:
    assert populated_mstorage

    def _run():
        response = client.get("/list")
        assert response.status_code == 200, response.json()

        assert response.json() == {
            "storages": [
                {
                    "backups": [
                        {
                            "attempt": 1,
                            "end": "2020-02-02T12:34:56Z",
                            "nodes": 1,
                            "files": 1,
                            "name": "1",
                            "plugin": "files",
                            "start": "2020-01-01T21:43:00Z",
                            "cluster_files": 1,
                            "cluster_data_size": 6,
                            "total_size": 6,
                            "upload_size": 6,
                            "upload_stored_size": 10,
                        },
                        {
                            "attempt": 1,
                            "end": "2020-02-02T12:34:56Z",
                            "nodes": 1,
                            "files": 1,
                            "name": "2",
                            "plugin": "files",
                            "start": "2020-01-01T21:43:00Z",
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
                            "end": "2020-02-02T12:34:56Z",
                            "nodes": 1,
                            "files": 1,
                            "name": "3",
                            "plugin": "files",
                            "start": "2020-01-01T21:43:00Z",
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
    num_files, total_size = compute_deduplicated_snapshot_file_stats(backup_manifest)
    assert (num_files, total_size) == (6, 6000)


def test_api_list_deduplication(backup_manifest: BackupManifest, tmp_path: Path) -> None:
    """Test the list backup operation correctly deduplicates snapshot files when computing stats."""
    multi_rohmu_storage = MultiRohmuStorage(config=create_rohmu_config(tmp_path))
    storage = multi_rohmu_storage.get_storage("x")
    storage.upload_json("backup-1", backup_manifest)
    storage.upload_hexdigest_bytes("FAKEDIGEST", b"fake-digest-data")

    list_request = ListRequest(storage="x")
    list_response = list_backups(req=list_request, json_mstorage=multi_rohmu_storage, cache={})
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


def test_list_can_use_cache_from_previous_response(backup_manifest: BackupManifest, tmp_path: Path) -> None:
    multi_rohmu_storage = MultiRohmuStorage(config=create_rohmu_config(tmp_path))
    storage = multi_rohmu_storage.get_storage("x")
    storage.upload_json("backup-1", backup_manifest)
    storage.upload_hexdigest_bytes("FAKEDIGEST", b"fake-digest-data")

    list_request = ListRequest(storage="x")
    first_list_response = list_backups(req=list_request, json_mstorage=multi_rohmu_storage, cache={})
    cached_entries = get_cache_entries_from_list_response(first_list_response)

    with mock.patch.object(storage, "download_json") as dowload_json:
        second_list_response = list_backups(req=list_request, json_mstorage=multi_rohmu_storage, cache=cached_entries)
        tested_entries = 0
        for storage_entry in second_list_response.storages:
            for backup_entry in storage_entry.backups:
                cached_entry = cached_entries[storage_entry.storage_name][backup_entry.name]
                assert backup_entry is cached_entry
                tested_entries += 1
        assert tested_entries > 0
        dowload_json.assert_not_called()


def test_list_does_not_return_stale_cache_entries(backup_manifest: BackupManifest, tmp_path: Path) -> None:
    multi_rohmu_storage = MultiRohmuStorage(config=create_rohmu_config(tmp_path))
    storage = multi_rohmu_storage.get_storage("x")
    storage.upload_json("backup-1", backup_manifest)
    storage.upload_hexdigest_bytes("FAKEDIGEST", b"fake-digest-data")

    list_request = ListRequest(storage="x")
    first_list_response = list_backups(req=list_request, json_mstorage=multi_rohmu_storage, cache={})
    cached_entries = get_cache_entries_from_list_response(first_list_response)
    storage.delete_json("backup-1")
    second_list_response = list_backups(req=list_request, json_mstorage=multi_rohmu_storage, cache=cached_entries)
    assert second_list_response.storages == [ListForStorage(storage_name="x", backups=[])]


def test_get_cache_entries_from_list_response() -> None:
    backup_x_1 = create_backup("backup_x_1")
    backup_x_2 = create_backup("backup_x_2")
    backup_y_3 = create_backup("backup_y_3")
    assert get_cache_entries_from_list_response(
        ListResponse(
            storages=[
                ListForStorage(storage_name="x", backups=[backup_x_1, backup_x_2]),
                ListForStorage(storage_name="y", backups=[backup_y_3]),
            ]
        )
    ) == {
        "x": {"backup_x_1": backup_x_1, "backup_x_2": backup_x_2},
        "y": {"backup_y_3": backup_y_3},
    }


def create_backup(name: str) -> ListSingleBackup:
    return ListSingleBackup(
        name=name,
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
