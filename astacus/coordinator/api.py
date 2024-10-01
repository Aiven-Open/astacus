"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from .cleanup import CleanupOp
from .coordinator import BackupOp, Coordinator, CoordinatorOp, DeltaBackupOp, RestoreOp
from .list import CachedListEntries, list_backups, list_delta_backups
from .lockops import LockOps
from .state import CachedListResponse
from astacus import config
from astacus.common import ipc
from astacus.common.magic import StrEnum
from astacus.common.op import Op
from astacus.common.progress import Progress
from astacus.config import APP_HASH_KEY, get_config_content_and_hash
from astacus.starlette import get_query_param, Router
from asyncio import to_thread
from starlette.background import BackgroundTasks
from starlette.exceptions import HTTPException
from starlette.requests import Request
from urllib.parse import urljoin

import logging
import msgspec
import os
import time

router = Router()

logger = logging.getLogger(__name__)


class OpName(StrEnum):
    """(Long-running) operations defined in this API (for coordinator)"""

    backup = "backup"
    lock = "lock"
    restore = "restore"
    unlock = "unlock"
    cleanup = "cleanup"


class LockStartResult(Op.StartResult):
    unlock_url: str


@router.get("/")
async def root():
    # Root is no-op, just useful for testing that Astacus is actually running
    return {}


@router.post("/config/reload")
async def config_reload(*, request: Request) -> dict:
    """Reload astacus configuration"""
    config_path = os.environ.get("ASTACUS_CONFIG")
    assert config_path is not None
    config.set_global_config_from_path(request.app, config_path)
    return {}


@router.get("/config/status")
async def config_status(*, request: Request) -> dict:
    config_path = os.environ.get("ASTACUS_CONFIG")
    assert config_path is not None
    _, config_hash = get_config_content_and_hash(config_path)
    loaded_config_hash = getattr(request.app.state, APP_HASH_KEY)
    return {"config_hash": loaded_config_hash, "needs_reload": config_hash != loaded_config_hash}


@router.post("/lock")
async def lock(*, request: Request, background_tasks: BackgroundTasks) -> LockStartResult:
    c = await Coordinator.create_from_request(request, background_tasks)
    locker = get_query_param(request, "locker")
    op = c.create_op(LockOps, locker=locker)
    result = c.start_op(op_name=OpName.lock, op=op, fun=op.lock)
    return LockStartResult(unlock_url=urljoin(str(c.request_url), f"../unlock?locker={locker}"), **result.dict())


@router.post("/unlock")
async def unlock(*, request: Request, background_tasks: BackgroundTasks) -> Op.StartResult:
    c = await Coordinator.create_from_request(request, background_tasks)
    locker = get_query_param(request, "locker")
    op = c.create_op(LockOps, locker=locker)
    return c.start_op(op_name=OpName.unlock, op=op, fun=op.unlock)


@router.post("/backup")
async def backup(*, request: Request, background_tasks: BackgroundTasks) -> Op.StartResult:
    c = await Coordinator.create_from_request(request, background_tasks)
    op = c.create_op(BackupOp)
    runner = await op.acquire_cluster_lock()
    return c.start_op(op_name=OpName.backup, op=op, fun=runner)


@router.post("/delta/backup")
async def delta_backup(*, request: Request, background_tasks: BackgroundTasks) -> Op.StartResult:
    c = await Coordinator.create_from_request(request, background_tasks)
    op = c.create_op(DeltaBackupOp)
    runner = await op.acquire_cluster_lock()
    return c.start_op(op_name=OpName.backup, op=op, fun=runner)


@router.post("/restore")
async def restore(*, body: ipc.RestoreRequest, request: Request, background_tasks: BackgroundTasks) -> Op.StartResult:
    c = await Coordinator.create_from_request(request, background_tasks)
    op = RestoreOp(c=c, req=body)
    runner = await op.acquire_cluster_lock()
    return c.start_op(op_name=OpName.restore, op=op, fun=runner)


@router.get("/list")
async def _list_backups(
    *, body: ipc.ListRequest = ipc.ListRequest(), request: Request, background_tasks: BackgroundTasks
) -> ipc.ListResponse:
    c = await Coordinator.create_from_request(request, background_tasks)
    coordinator_config = c.config
    cached_list_response = c.state.cached_list_response
    if cached_list_response is not None:
        age = time.monotonic() - cached_list_response.timestamp
        if (
            age < c.config.list_ttl
            and cached_list_response.coordinator_config == coordinator_config
            and cached_list_response.list_request
        ):
            return cached_list_response.list_response
    if c.state.cached_list_running:
        raise HTTPException(status_code=429, detail="Already caching list result")
    c.state.cached_list_running = True
    try:
        cache = (
            get_cache_entries_from_list_response(cached_list_response.list_response)
            if cached_list_response is not None
            else {}
        )
        list_response = await to_thread(list_backups, req=body, storage_factory=c.storage_factory, cache=cache)
        c.state.cached_list_response = CachedListResponse(
            coordinator_config=coordinator_config,
            list_request=body,
            list_response=list_response,
        )
    finally:
        c.state.cached_list_running = False
    return list_response


def get_cache_entries_from_list_response(list_response: ipc.ListResponse) -> CachedListEntries:
    return {
        listed_storage.storage_name: {listed_backup.name: listed_backup for listed_backup in listed_storage.backups}
        for listed_storage in list_response.storages
    }


@router.get("/delta/list")
async def _list_delta_backups(
    *, body: ipc.ListRequest, request: Request, background_tasks: BackgroundTasks
) -> ipc.ListResponse:
    c = await Coordinator.create_from_request(request, background_tasks)
    # This is not supposed to be called very often, no caching necessary
    return await to_thread(list_delta_backups, req=body, storage_factory=c.storage_factory)


@router.post("/cleanup")
async def cleanup(
    *, request: Request, background_tasks: BackgroundTasks, body: ipc.CleanupRequest = ipc.CleanupRequest()
) -> Op.StartResult:
    c = await Coordinator.create_from_request(request, background_tasks)
    op = CleanupOp(c=c, req=body)
    runner = await op.acquire_cluster_lock()
    return c.start_op(op_name=OpName.cleanup, op=op, fun=runner)


class OpStatusResult(msgspec.Struct, kw_only=True):
    state: Op.Status | None
    progress: Progress | None


@router.get("/{op_name:str}/{op_id:int}")
@router.get("/delta/{op_name:str}/{op_id:int}")
async def op_status(*, request: Request, background_tasks: BackgroundTasks) -> OpStatusResult:
    c = await Coordinator.create_from_request(request, background_tasks)
    op_name = OpName(request.path_params["op_name"])
    op_id: int = request.path_params["op_id"]
    op, op_info = c.get_op_and_op_info(op_id=op_id, op_name=op_name)
    result = OpStatusResult(state=op_info.op_status, progress=None)
    if isinstance(op, BackupOp | DeltaBackupOp | RestoreOp):
        result.progress = op.progress
    return result


@router.put("/{op_name:str}/{op_id:int}/sub-result")
@router.put("/delta/{op_name:str}/{op_id:int}/sub-result")
async def op_sub_result(*, request: Request, background_tasks: BackgroundTasks) -> None:
    c = await Coordinator.create_from_request(request, background_tasks)
    op_name = OpName(request.path_params["op_name"])
    op_id: int = request.path_params["op_id"]
    op, _ = c.get_op_and_op_info(op_id=op_id, op_name=op_name)
    assert isinstance(op, CoordinatorOp)
    # We used to have results available here, but not use those
    # that was wasting a lot of memory by generating the same result twice.
    if not op.subresult_sleeper:
        return
    op.subresult_sleeper.wakeup()


@router.get("/busy")
async def is_busy(*, request: Request, background_tasks: BackgroundTasks) -> bool:
    c = await Coordinator.create_from_request(request, background_tasks)
    return c.is_busy()
