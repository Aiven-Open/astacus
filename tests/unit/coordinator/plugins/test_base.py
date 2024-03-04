"""

Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from astacus.common import ipc, utils
from astacus.common.asyncstorage import AsyncHexDigestStorage, AsyncJsonStorage
from astacus.common.ipc import ManifestMin, Plugin, SnapshotHash
from astacus.common.op import Op
from astacus.common.progress import Progress
from astacus.common.utils import now
from astacus.coordinator.cluster import Cluster
from astacus.coordinator.config import CoordinatorNode
from astacus.coordinator.plugins.base import (
    BackupManifestStep,
    ComputeKeptBackupsStep,
    DeleteBackupAndDeltaManifestsStep,
    DeleteBackupManifestsStep,
    DeleteDanglingHexdigestsStep,
    DeltaManifestsStep,
    ListBackupsStep,
    ListDeltaBackupsStep,
    ListHexdigestsStep,
    SnapshotReleaseStep,
    SnapshotStep,
    Step,
    StepsContext,
    UploadBlocksStep,
    UploadManifestStep,
)
from astacus.node.snapshotter import hash_hexdigest_readable
from collections.abc import Callable, Sequence, Set
from http import HTTPStatus
from io import BytesIO
from tests.unit.storage import MemoryHexDigestStorage, MemoryJsonStorage
from unittest import mock

import dataclasses
import datetime
import httpx
import json
import msgspec
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
    end: datetime.datetime | None = msgspec.field(default_factory=now)
    hashes: Sequence[ipc.SnapshotHash] | None = None


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


def make_manifest(start: str, end: str, *, filename: str = "") -> ipc.BackupManifest:
    manifest = ipc.BackupManifest(
        start=datetime.datetime.fromisoformat(start),
        end=datetime.datetime.fromisoformat(end),
        attempt=1,
        snapshot_results=[],
        upload_results=[],
        plugin=Plugin.files,
        filename=filename,
    )
    return manifest


def named_manifest(filename: str) -> ipc.BackupManifest:
    some_isoformat = "2020-01-05T15:30Z"
    return make_manifest(some_isoformat, some_isoformat, filename=filename)


def manifest_with_hashes(hashes: dict[str, bytes], index: int) -> ipc.BackupManifest:
    return ipc.BackupManifest(
        start=datetime.datetime.fromisoformat("2020-01-05T15:30Z"),
        end=datetime.datetime.fromisoformat("2020-01-05T15:30Z"),
        attempt=1,
        snapshot_results=[
            DefaultedSnapshotResult(hashes=[SnapshotHash(hexdigest=h, size=len(v)) for h, v in hashes.items()])
        ],
        upload_results=[],
        plugin=Plugin.files,
        filename=f"some-manifest-{index}",
    )


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
    context.set_result(ListHexdigestsStep, set())
    sample_hashes = get_sample_hashes()
    context.set_result(
        SnapshotStep, [ipc.SnapshotResult(hostname="localhost", az="az1", files=1, total_size=2, hashes=sample_hashes)]
    )
    upload_step = UploadBlocksStep(storage_name="fake")
    with respx.mock:
        metadata_request = respx.get("http://node_1/metadata").respond(
            json=msgspec.to_builtins(
                ipc.MetadataResult(version="0.1", features=[feature.value for feature in node_features])
            ),
        )
        upload_request = respx.post("http://node_1/upload").mock(
            side_effect=make_request_check(msgspec.to_builtins(expected_request), "upload")
        )
        status_request = respx.get("http://node_1/upload/1").respond(
            json=msgspec.to_builtins(
                ipc.SnapshotUploadResult(
                    hostname="localhost",
                    az="az1",
                    progress=Progress(handled=1, total=1, final=True),
                    total_size=10,
                    total_stored_size=8,
                )
            ),
        )
        await upload_step.run_step(cluster=single_node_cluster, context=context)
        assert metadata_request.call_count == 1
        assert upload_request.call_count == 1
        assert status_request.called


BACKUPS_FOR_RETENTION_TEST = {
    "b1": msgspec.json.encode(make_manifest("2020-01-01T11:00Z", "2020-01-01T13:00Z")),
    "b2": msgspec.json.encode(make_manifest("2020-01-02T11:00Z", "2020-01-02T13:00Z")),
    "b3": msgspec.json.encode(make_manifest("2020-01-03T11:00Z", "2020-01-03T13:00Z")),
    "b4": msgspec.json.encode(make_manifest("2020-01-04T11:00Z", "2020-01-04T13:00Z")),
    "b5": msgspec.json.encode(make_manifest("2020-01-05T11:00Z", "2020-01-05T13:00Z")),
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
    kept_backups: Set[str],
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
    assert {b.filename for b in result} == kept_backups


BACKUPS_FOR_DELTA_RETENTION_TEST = {
    "delta-0": make_manifest("2020-01-01T03:30Z", "2020-01-01T03:40Z"),
    "b1": make_manifest("2020-01-01T11:00Z", "2020-01-01T13:00Z"),
    "b2": make_manifest("2020-01-02T11:00Z", "2020-01-02T13:00Z"),
    "delta-231": make_manifest("2020-01-02T13:30Z", "2020-01-02T13:40Z"),
    "delta-232": make_manifest("2020-01-02T15:30Z", "2020-01-02T15:40Z"),
    "b3": make_manifest("2020-01-03T11:00Z", "2020-01-03T13:00Z"),
    "b4": make_manifest("2020-01-04T11:00Z", "2020-01-04T13:00Z"),
    "delta-451": make_manifest("2020-01-04T15:30Z", "2020-01-04T15:40Z"),
    "b5": make_manifest("2020-01-05T11:00Z", "2020-01-05T13:00Z"),
    "delta-561": make_manifest("2020-01-05T15:30Z", "2020-01-05T15:40Z"),
}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "explicit_delete,expected_kept_backups",
    [
        ([], {"b1", "b2", "delta-231", "delta-232", "b3", "b4", "delta-451", "b5", "delta-561"}),
        (["b1"], {"b2", "delta-231", "delta-232", "b3", "b4", "delta-451", "b5", "delta-561"}),
        (["b1", "b2"], {"b3", "b4", "delta-451", "b5", "delta-561"}),
        (["b1", "b2", "b3"], {"b4", "delta-451", "b5", "delta-561"}),
        (["b1", "b2", "b3", "b4"], {"b5", "delta-561"}),
        (["b1", "b2", "b3", "b4", "b5"], set()),
    ],
)
async def test_compute_kept_deltas(
    explicit_delete: Sequence[str],
    expected_kept_backups: Set[str],
    single_node_cluster: Cluster,
    context: StepsContext,
):
    async_json_storage = AsyncJsonStorage(
        storage=MemoryJsonStorage(items={k: msgspec.json.encode(v) for k, v in BACKUPS_FOR_DELTA_RETENTION_TEST.items()})
    )
    keep_all_retention = ipc.Retention(minimum_backups=len(BACKUPS_FOR_DELTA_RETENTION_TEST))
    context.set_result(ListBackupsStep, {"b1", "b2", "b3", "b4", "b5"})
    context.set_result(ListDeltaBackupsStep, {"delta-0", "delta-231", "delta-232", "delta-451", "delta-561"})
    step = ComputeKeptBackupsStep(
        json_storage=async_json_storage,
        retention=keep_all_retention,
        explicit_delete=explicit_delete,
        retain_deltas=True,
    )
    kept_backups = await step.run_step(single_node_cluster, context)
    assert {b.filename for b in kept_backups} == expected_kept_backups


@pytest.mark.asyncio
async def test_delete_backup_manifests(single_node_cluster: Cluster, context: StepsContext) -> None:
    json_items: dict[str, bytes] = {
        "b1": b"",
        "d1": b"",
        "b2": b"",
    }
    async_json_storage = AsyncJsonStorage(storage=MemoryJsonStorage(items=json_items))
    context.set_result(ListBackupsStep, {"b1", "b2"})
    context.set_result(ComputeKeptBackupsStep, [named_manifest("b2")])
    step = DeleteBackupManifestsStep(async_json_storage)
    deleted_backups = await step.run_step(single_node_cluster, context)
    assert deleted_backups == {"b1"}
    assert set(json_items.keys()) == {"d1", "b2"}


@dataclasses.dataclass
class DeleteBackupManifestsParam:
    all_backups: set[str]
    all_deltas: set[str]
    kept_backups: Sequence[ipc.BackupManifest]
    expected_deleted: set[str]
    expected_remaining: set[str]
    test_id: str


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "p",
    [
        DeleteBackupManifestsParam(
            test_id="both base and deltas",
            all_backups={"b1", "b2"},
            all_deltas={"d1", "d2"},
            kept_backups=[named_manifest("b2"), named_manifest("d2")],
            expected_deleted={"b1", "d1"},
            expected_remaining={"b2", "d2"},
        ),
        DeleteBackupManifestsParam(
            test_id="no deltas",
            all_backups={"b1", "b2"},
            all_deltas=set(),
            kept_backups=[named_manifest("b2")],
            expected_deleted={"b1"},
            expected_remaining={"b2"},
        ),
    ],
    ids=lambda p: p.test_id,
)
async def test_delete_backup_and_delta_manifests(
    single_node_cluster: Cluster, context: StepsContext, p: DeleteBackupManifestsParam
) -> None:
    json_items = {b: msgspec.json.encode(named_manifest(b)) for b in (p.all_backups | p.all_deltas)}
    async_json_storage = AsyncJsonStorage(storage=MemoryJsonStorage(items=json_items))
    context.set_result(ListBackupsStep, p.all_backups)
    context.set_result(ListDeltaBackupsStep, p.all_deltas)
    context.set_result(ComputeKeptBackupsStep, p.kept_backups)
    step = DeleteBackupAndDeltaManifestsStep(async_json_storage)
    deleted_backups = await step.run_step(single_node_cluster, context)
    assert deleted_backups == p.expected_deleted
    assert set(json_items.keys()) == p.expected_remaining


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kept_backups,stored_hashes,expected_hashes",
    [
        ([], {}, {}),
        ([], {"a": b"a"}, {}),
        ([], {"a": b"a"}, {}),
        (
            [manifest_with_hashes({"a": b"a"}, 0), manifest_with_hashes({"b": b"b"}, 1)],
            {"a": b"a", "b": b"b", "c": b"c"},
            {"a": b"a", "b": b"b"},
        ),
    ],
)
async def test_delete_dangling_hexdigests_step(
    single_node_cluster: Cluster,
    context: StepsContext,
    kept_backups: Sequence[ipc.BackupManifest],
    stored_hashes: dict[str, bytes],
    expected_hashes: dict[str, bytes],
) -> None:
    async_digest_storage = AsyncHexDigestStorage(storage=MemoryHexDigestStorage(items=stored_hashes))
    async_json_storage = AsyncJsonStorage(
        storage=MemoryJsonStorage(items={b.filename: msgspec.json.encode(b) for b in kept_backups})
    )
    context.set_result(ComputeKeptBackupsStep, [ManifestMin.from_manifest(b) for b in kept_backups])
    step = DeleteDanglingHexdigestsStep(json_storage=async_json_storage, hexdigest_storage=async_digest_storage)
    await step.run_step(single_node_cluster, context)
    assert stored_hashes == expected_hashes


@pytest.mark.asyncio
async def test_delete_backup_and_delta_manifests_raises_when_delta_steps_are_missing(
    single_node_cluster: Cluster, context: StepsContext
) -> None:
    async_json_storage = AsyncJsonStorage(storage=MemoryJsonStorage(items={}))
    context.set_result(ListBackupsStep, set())
    context.set_result(ComputeKeptBackupsStep, [])
    step = DeleteBackupAndDeltaManifestsStep(async_json_storage)
    with pytest.raises(Exception):
        await step.run_step(single_node_cluster, context)


@pytest.mark.asyncio
async def test_upload_manifest_step_generates_correct_backup_name(
    single_node_cluster: Cluster,
    context: StepsContext,
) -> None:
    context.attempt_start = datetime.datetime(2020, 1, 7, 5, 0, tzinfo=datetime.timezone.utc)
    context.set_result(SnapshotStep, [DefaultedSnapshotResult()])
    context.set_result(UploadBlocksStep, [ipc.SnapshotUploadResult()])
    async_json_storage = AsyncJsonStorage(storage=MemoryJsonStorage(items={}))
    step = UploadManifestStep(json_storage=async_json_storage, plugin=Plugin.files)
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
    expected_request: ipc.SnapshotReleaseRequest | None,
    single_node_cluster: Cluster,
    context: StepsContext,
) -> None:
    hashes_to_release = [ipc.SnapshotHash(hexdigest="aaa", size=1), ipc.SnapshotHash(hexdigest="bbb", size=2)]
    context.set_result(SnapshotStep, [DefaultedSnapshotResult(hashes=hashes_to_release)])
    release_step = SnapshotReleaseStep()

    with respx.mock:
        metadata_request = respx.get("http://node_1/metadata").respond(
            json=msgspec.to_builtins(
                ipc.MetadataResult(version="0.1", features=[feature.value for feature in node_features])
            )
        )
        if ipc.NodeFeatures.release_snapshot_files in node_features:
            assert expected_request is not None
            release_request = respx.post("http://node_1/release").mock(
                side_effect=make_request_check(msgspec.to_builtins(expected_request), "release")
            )
            status_request = respx.get("http://node_1/release/1").respond(
                json=msgspec.to_builtins(
                    ipc.NodeResult(
                        hostname="localhost",
                        az="az1",
                        progress=Progress(handled=2, total=2, final=True),
                    )
                ),
            )
        await release_step.run_step(cluster=single_node_cluster, context=context)
        assert metadata_request.call_count == 1
        if ipc.NodeFeatures.release_snapshot_files in node_features:
            assert release_request.call_count == 1
            assert status_request.called


@dataclasses.dataclass
class TestListDeltasParam:
    test_id: str
    basebackup_manifest: ipc.BackupManifest
    stored_jsons: dict[str, bytes]
    expected_deltas: list[str]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "p",
    [
        TestListDeltasParam(
            test_id="empty_storage",
            basebackup_manifest=make_manifest(start="1970-01-01T00:00", end="1970-01-01T00:30"),
            stored_jsons={},
            expected_deltas=[],
        ),
        TestListDeltasParam(
            test_id="single_delta",
            basebackup_manifest=make_manifest(start="1970-01-01T00:00", end="1970-01-01T00:30"),
            stored_jsons={
                "backup-base": msgspec.json.encode(make_manifest(start="1970-01-01T00:00", end="1970-01-01T00:30")),
                "delta-one": msgspec.json.encode(make_manifest(start="1970-01-01T01:00", end="1970-01-01T01:05")),
            },
            expected_deltas=["delta-one"],
        ),
        TestListDeltasParam(
            test_id="deltas_older_than_backup_are_not_listed",
            basebackup_manifest=make_manifest(start="2000-01-01T00:00", end="2000-01-01T00:30"),
            stored_jsons={
                "backup-old": msgspec.json.encode(make_manifest(start="1970-01-01T00:00", end="1970-01-01T00:30")),
                "delta-old": msgspec.json.encode(make_manifest(start="1970-01-01T01:00", end="1970-01-01T01:05")),
                "backup-one": msgspec.json.encode(make_manifest(start="2000-01-01T00:00", end="2000-01-01T00:30")),
                "delta-one": msgspec.json.encode(make_manifest(start="2000-01-01T01:00", end="2000-01-01T01:05")),
                "delta-two": msgspec.json.encode(make_manifest(start="2000-01-01T12:00", end="1970-01-01T12:05")),
                "backup-two": msgspec.json.encode(make_manifest(start="2000-01-02T00:00", end="2000-01-02T00:30")),
                "delta-three": msgspec.json.encode(make_manifest(start="2000-01-02T12:00", end="2000-01-02T12:05")),
            },
            expected_deltas=["delta-one", "delta-two", "delta-three"],
        ),
        TestListDeltasParam(
            test_id="relies_on_start_time_in_case_of_intersections",
            basebackup_manifest=make_manifest(start="2000-01-01T00:00", end="2000-01-01T00:30"),
            stored_jsons={
                "delta-old": msgspec.json.encode(make_manifest(start="1999-12-31T23:59", end="2000-01-01T00:04")),
                "backup-one": msgspec.json.encode(make_manifest(start="2000-01-01T00:00", end="2000-01-01T00:30")),
                "delta-one": msgspec.json.encode(make_manifest(start="2000-01-01T00:05", end="2000-01-01T00:10")),
            },
            expected_deltas=["delta-one"],
        ),
    ],
    ids=lambda p: p.test_id,
)
async def test_list_delta_backups(p: TestListDeltasParam) -> None:
    async_json_storage = AsyncJsonStorage(MemoryJsonStorage(p.stored_jsons))
    step = DeltaManifestsStep(async_json_storage)
    cluster = Cluster(nodes=[CoordinatorNode(url="http://node_1")])
    context = StepsContext()
    context.set_result(BackupManifestStep, p.basebackup_manifest)
    backup_names = [b.filename for b in await step.run_step(cluster=cluster, context=context)]
    assert backup_names == p.expected_deltas
