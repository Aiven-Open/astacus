"""

Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from astacus.common import ipc, utils
from astacus.common.asyncstorage import AsyncJsonStorage
from astacus.common.op import Op
from astacus.common.progress import Progress
from astacus.common.utils import now
from astacus.coordinator.cluster import Cluster
from astacus.coordinator.config import CoordinatorNode
from astacus.coordinator.plugins.base import (
    ComputeKeptBackupsStep,
    ListBackupsStep,
    ListHexdigestsStep,
    SnapshotReleaseStep,
    SnapshotStep,
    Step,
    StepsContext,
    UploadBlocksStep,
    UploadManifestStep,
)
from astacus.node.snapshotter import hash_hexdigest_readable
from http import HTTPStatus
from io import BytesIO
from pydantic import Field
from tests.unit.json_storage import MemoryJsonStorage
from typing import AbstractSet, Callable, List, Optional, Sequence
from unittest import mock

import datetime
import httpx
import json
import pytest
import respx


class DummyStep(Step[int]):
    async def run_step(self, cluster: Cluster, context: StepsContext) -> int:
        return 1


def test_steps_context_result_can_be_set_and_retrieved():
    context = StepsContext()
    context.set_result(DummyStep, 10)
    assert context.get_result(DummyStep) == 10


def test_steps_context_missing_result_fails():
    context = StepsContext()
    with pytest.raises(LookupError):
        context.get_result(DummyStep)


def test_steps_context_result_can_only_be_set_once():
    context = StepsContext()
    context.set_result(DummyStep, 10)
    with pytest.raises(RuntimeError):
        context.set_result(DummyStep, 10)


def get_sample_hashes() -> list[ipc.SnapshotHash]:
    data = b"new_data"
    hexdigest = hash_hexdigest_readable(BytesIO(data))
    return [ipc.SnapshotHash(hexdigest=hexdigest, size=len(data))]


class DefaultedSnapshotResult(ipc.SnapshotResult):
    end: Optional[datetime.datetime] = Field(default_factory=now)
    hashes: Optional[List[ipc.SnapshotHash]] = None


@pytest.fixture(name="single_node_cluster")
def fixture_single_node_cluster() -> Cluster:
    return Cluster(nodes=[CoordinatorNode(url="http://node_1")])


@pytest.fixture(name="context")
def fixture_context() -> StepsContext:
    return StepsContext()


def make_request_check(expected_payload: dict, op_name: str) -> Callable[[httpx.Request], httpx.Response]:
    def check_request(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        assert payload == expected_payload
        return httpx.Response(
            status_code=HTTPStatus.OK, json=Op.StartResult(op_id=1, status_url=f"http://node_1/{op_name}/1").jsondict()
        )

    return check_request


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "node_features,expected_request",
    [
        (
            [],
            ipc.SnapshotUploadRequest(result_url="", hashes=get_sample_hashes(), storage="fake"),
        ),
        (
            [ipc.NodeFeatures.validate_file_hashes],
            ipc.SnapshotUploadRequestV20221129(
                result_url="", hashes=get_sample_hashes(), storage="fake", validate_file_hashes=True
            ),
        ),
    ],
    ids=["no_feature", "validate_file_hashes"],
)
async def test_upload_step_uses_new_request_if_supported(
    node_features: Sequence[ipc.NodeFeatures],
    expected_request: ipc.SnapshotUploadRequest,
    single_node_cluster: Cluster,
    context: StepsContext,
) -> None:
    context.set_result(ListHexdigestsStep, {})
    sample_hashes = get_sample_hashes()
    context.set_result(
        SnapshotStep, [ipc.SnapshotResult(hostname="localhost", az="az1", files=1, total_size=2, hashes=sample_hashes)]
    )
    upload_step = UploadBlocksStep(storage_name="fake")
    with respx.mock:
        metadata_request = respx.get("http://node_1/metadata").respond(
            json=ipc.MetadataResult(version="0.1", features=[feature.value for feature in node_features]).jsondict()
        )
        upload_request = respx.post("http://node_1/upload").mock(
            side_effect=make_request_check(expected_request.jsondict(), "upload")
        )
        status_request = respx.get("http://node_1/upload/1").respond(
            json=ipc.SnapshotUploadResult(
                hostname="localhost",
                az="az1",
                progress=Progress(handled=1, total=1, final=True),
                total_size=10,
                total_stored_size=8,
            ).jsondict()
        )
        await upload_step.run_step(cluster=single_node_cluster, context=context)
        assert metadata_request.call_count == 1
        assert upload_request.call_count == 1
        assert status_request.called


BACKUPS_FOR_RETENTION_TEST = {
    "b1": json.dumps(
        {
            "start": "2020-01-01T11:00Z",
            "end": "2020-01-01T13:00Z",
            "attempt": 1,
            "snapshot_results": [],
            "upload_results": [],
            "plugin": "clickhouse",
        }
    ),
    "b2": json.dumps(
        {
            "start": "2020-01-02T11:00Z",
            "end": "2020-01-02T13:00Z",
            "attempt": 1,
            "snapshot_results": [],
            "upload_results": [],
            "plugin": "clickhouse",
        }
    ),
    "b3": json.dumps(
        {
            "start": "2020-01-03T11:00Z",
            "end": "2020-01-03T13:00Z",
            "attempt": 1,
            "snapshot_results": [],
            "upload_results": [],
            "plugin": "clickhouse",
        }
    ),
    "b4": json.dumps(
        {
            "start": "2020-01-04T11:00Z",
            "end": "2020-01-04T13:00Z",
            "attempt": 1,
            "snapshot_results": [],
            "upload_results": [],
            "plugin": "clickhouse",
        }
    ),
    "b5": json.dumps(
        {
            "start": "2020-01-05T11:00Z",
            "end": "2020-01-05T13:00Z",
            "attempt": 1,
            "snapshot_results": [],
            "upload_results": [],
            "plugin": "clickhouse",
        }
    ),
}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "retention,kept_backups",
    [
        (ipc.Retention(minimum_backups=1, maximum_backups=6, keep_days=6), {"b1", "b2", "b3", "b4", "b5"}),
        (ipc.Retention(minimum_backups=1, maximum_backups=3, keep_days=6), {"b3", "b4", "b5"}),
        (ipc.Retention(minimum_backups=1, maximum_backups=3, keep_days=2), {"b4", "b5"}),
        (ipc.Retention(minimum_backups=1, maximum_backups=3, keep_days=1), {"b5"}),
        (ipc.Retention(minimum_backups=1, maximum_backups=3, keep_days=0), {"b5"}),
        (ipc.Retention(minimum_backups=0, maximum_backups=3, keep_days=0), set()),
    ],
)
async def test_compute_kept_backups(
    retention: ipc.Retention,
    kept_backups: AbstractSet[str],
    single_node_cluster: Cluster,
    context: StepsContext,
) -> None:
    async_json_storage = AsyncJsonStorage(storage=MemoryJsonStorage(items=BACKUPS_FOR_RETENTION_TEST))
    step = ComputeKeptBackupsStep(
        json_storage=async_json_storage,
        retention=retention,
        explicit_delete=[],
    )
    context.set_result(ListBackupsStep, set(await async_json_storage.list_jsons()))
    half_a_day_after_last_backup = datetime.datetime(2020, 1, 7, 5, 0, tzinfo=datetime.timezone.utc)
    with mock.patch.object(utils, "now", lambda: half_a_day_after_last_backup):
        result = await step.run_step(cluster=single_node_cluster, context=context)
    assert result == kept_backups


@pytest.mark.asyncio
async def test_upload_manifest_step_generates_correct_backup_name(
    single_node_cluster: Cluster,
    context: StepsContext,
) -> None:
    context.attempt_start = datetime.datetime(2020, 1, 7, 5, 0, tzinfo=datetime.timezone.utc)
    context.set_result(SnapshotStep, [DefaultedSnapshotResult()])
    context.set_result(UploadBlocksStep, [ipc.SnapshotUploadResult()])
    async_json_storage = AsyncJsonStorage(storage=MemoryJsonStorage(items={}))
    step = UploadManifestStep(json_storage=async_json_storage, plugin=ipc.Plugin.files)
    await step.run_step(cluster=single_node_cluster, context=context)
    assert isinstance(async_json_storage.storage, MemoryJsonStorage)
    assert "backup-2020-01-07T05:00:00+00:00" in async_json_storage.storage.items


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "node_features,expected_request",
    [
        ([], None),
        (
            [ipc.NodeFeatures.release_snapshot_files],
            ipc.SnapshotReleaseRequest(hexdigests=["aaa", "bbb"]),
        ),
    ],
)
async def test_snapshot_release_step(
    node_features: Sequence[ipc.NodeFeatures],
    expected_request: Optional[ipc.SnapshotReleaseRequest],
    single_node_cluster: Cluster,
    context: StepsContext,
) -> None:
    hashes_to_release = [ipc.SnapshotHash(hexdigest="aaa", size=1), ipc.SnapshotHash(hexdigest="bbb", size=2)]
    context.set_result(SnapshotStep, [DefaultedSnapshotResult(hashes=hashes_to_release)])
    release_step = SnapshotReleaseStep()

    with respx.mock:
        metadata_request = respx.get("http://node_1/metadata").respond(
            json=ipc.MetadataResult(version="0.1", features=[feature.value for feature in node_features]).jsondict()
        )
        if ipc.NodeFeatures.release_snapshot_files in node_features:
            assert expected_request is not None
            release_request = respx.post("http://node_1/release").mock(
                side_effect=make_request_check(expected_request.jsondict(), "release")
            )
            status_request = respx.get("http://node_1/release/1").respond(
                json=ipc.NodeResult(
                    hostname="localhost",
                    az="az1",
                    progress=Progress(handled=2, total=2, final=True),
                ).jsondict()
            )
        await release_step.run_step(cluster=single_node_cluster, context=context)
        assert metadata_request.call_count == 1
        if ipc.NodeFeatures.release_snapshot_files in node_features:
            assert release_request.call_count == 1
            assert status_request.called
