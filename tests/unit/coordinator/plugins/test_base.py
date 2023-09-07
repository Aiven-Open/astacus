"""

Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from astacus.common import ipc, utils
from astacus.common.asyncstorage import AsyncJsonStorage
from astacus.common.op import Op
from astacus.common.progress import Progress
from astacus.coordinator.cluster import Cluster
from astacus.coordinator.config import CoordinatorNode
from astacus.coordinator.plugins.base import (
    ComputeKeptBackupsStep,
    ListBackupsStep,
    ListHexdigestsStep,
    SnapshotStep,
    Step,
    StepsContext,
    UploadBlocksStep,
)
from astacus.node.api import Features
from astacus.node.snapshotter import hash_hexdigest_readable
from http import HTTPStatus
from io import BytesIO
from tests.unit.json_storage import MemoryJsonStorage
from typing import AbstractSet, Sequence
from unittest import mock

import datetime
import httpx
import json
import pytest
import respx


class DummyStep(Step[int]):
    async def run_step(self, cluster: Cluster, context: StepsContext) -> int:
        return 1


def test_steps_context_backup_name_is_prefixed_timestamp():
    context = StepsContext(attempt_start=datetime.datetime(2020, 1, 2, 3, 4, 5, 678912, tzinfo=datetime.timezone.utc))
    assert context.backup_name == "backup-2020-01-02T03:04:05+00:00"


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


@pytest.fixture(name="single_node_cluster")
def fixture_single_node_cluster() -> Cluster:
    return Cluster(nodes=[CoordinatorNode(url="http://node_1")])


@pytest.fixture(name="context")
def fixture_context() -> StepsContext:
    return StepsContext()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "node_features,expected_request",
    [
        (
            [],
            ipc.SnapshotUploadRequest(result_url="", hashes=get_sample_hashes(), storage="fake"),
        ),
        (
            [Features.validate_file_hashes],
            ipc.SnapshotUploadRequestV20221129(
                result_url="", hashes=get_sample_hashes(), storage="fake", validate_file_hashes=True
            ),
        ),
    ],
    ids=["no_feature", "validate_file_hashes"],
)
async def test_upload_step_uses_new_request_if_supported(
    node_features: Sequence[Features],
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

        def check_request(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content)
            assert payload == expected_request.jsondict()
            return httpx.Response(
                status_code=HTTPStatus.OK, json=Op.StartResult(op_id=1, status_url="http://node_1/upload/1").jsondict()
            )

        respx.get("http://node_1/metadata").respond(
            json=ipc.MetadataResult(version="0.1", features=[feature.value for feature in node_features]).jsondict()
        )
        respx.post("http://node_1/upload").mock(side_effect=check_request)
        respx.get("http://node_1/upload/1").respond(
            json=ipc.SnapshotUploadResult(
                hostname="localhost",
                az="az1",
                progress=Progress(handled=1, total=1, final=True),
                total_size=10,
                total_stored_size=8,
            ).jsondict()
        )
        await upload_step.run_step(cluster=single_node_cluster, context=context)


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
