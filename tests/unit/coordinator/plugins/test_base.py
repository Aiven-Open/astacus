"""

Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from astacus.common import ipc
from astacus.common.op import Op
from astacus.common.progress import Progress
from astacus.coordinator.cluster import Cluster
from astacus.coordinator.config import CoordinatorNode
from astacus.coordinator.plugins.base import ListHexdigestsStep, SnapshotStep, Step, StepsContext, UploadBlocksStep
from astacus.node.api import Features
from astacus.node.snapshotter import hash_hexdigest_readable
from http import HTTPStatus
from io import BytesIO
from typing import Sequence

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
    node_features: Sequence[Features], expected_request: ipc.SnapshotUploadRequest
) -> None:
    context = StepsContext()
    context.set_result(ListHexdigestsStep, {})
    sample_hashes = get_sample_hashes()
    context.set_result(
        SnapshotStep, [ipc.SnapshotResult(hostname="localhost", az="az1", files=1, total_size=2, hashes=sample_hashes)]
    )
    upload_step = UploadBlocksStep(storage_name="fake")
    coordinator_nodes = [CoordinatorNode(url="http://node_1")]
    cluster = Cluster(nodes=coordinator_nodes)
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
        await upload_step.run_step(cluster=cluster, context=context)
