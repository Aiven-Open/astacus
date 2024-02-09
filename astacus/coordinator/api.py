"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""
from .cleanup import CleanupOp
from .coordinator import BackupOp, Coordinator, DeltaBackupOp, RestoreOp
from .list import list_backups, list_delta_backups
from .lockops import LockOps
from .state import CachedListResponse
from astacus import config
from astacus.common import ipc
from astacus.common.magic import StrEnum
from astacus.common.op import Op
from astacus.config import APP_HASH_KEY, get_config_content_and_hash
from asyncio import to_thread
from collections.abc import Sequence
from fastapi import APIRouter, Body, Depends, HTTPException, Request
from typing import Annotated, Optional
from urllib.parse import urljoin

import logging
import msgspec
import os
import time

router = APIRouter()

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
async def config_reload(*, request: Request, c: Coordinator = Depends()):
    """Reload astacus configuration"""
    config_path = os.environ.get("ASTACUS_CONFIG")
    assert config_path is not None
    config.set_global_config_from_path(request.app, config_path)
    return {}


@router.get("/config/status")
async def config_status(*, request: Request):
    config_path = os.environ.get("ASTACUS_CONFIG")
    assert config_path is not None
    _, config_hash = get_config_content_and_hash(config_path)
    loaded_config_hash = getattr(request.app.state, APP_HASH_KEY)
    return {"config_hash": loaded_config_hash, "needs_reload": config_hash != loaded_config_hash}


@router.post("/lock")
async def lock(*, locker: str, c: Coordinator = Depends(), op: LockOps = Depends()):
    result = c.start_op(op_name=OpName.lock, op=op, fun=op.lock)
    return msgspec.to_builtins(
        LockStartResult(unlock_url=urljoin(str(c.request_url), f"../unlock?locker={locker}"), **msgspec.to_builtins(result))
    )


@router.post("/unlock")
def unlock(*, locker: str, c: Coordinator = Depends(), op: LockOps = Depends()):
    return c.start_op(op_name=OpName.unlock, op=op, fun=op.unlock)


@router.post("/backup")
async def backup(*, c: Coordinator = Depends(), op: BackupOp = Depends(BackupOp.create)):
    runner = await op.acquire_cluster_lock()
    return c.start_op(op_name=OpName.backup, op=op, fun=runner)


@router.post("/delta/backup")
async def delta_backup(*, c: Coordinator = Depends(), op: DeltaBackupOp = Depends(DeltaBackupOp.create)):
    runner = await op.acquire_cluster_lock()
    return c.start_op(op_name=OpName.backup, op=op, fun=runner)


@router.post("/restore")
async def restore(
    *,
    c: Coordinator = Depends(),
    storage: Annotated[str, Body()] = "",
    name: Annotated[str, Body()] = "",
    partial_restore_nodes: Annotated[Optional[Sequence[ipc.PartialRestoreRequestNode]], Body()] = None,
    stop_after_step: Annotated[Optional[str], Body()] = None,
):
    req = ipc.RestoreRequest(
        storage=storage,
        name=name,
        partial_restore_nodes=partial_restore_nodes,
        stop_after_step=stop_after_step,
    )
    op = RestoreOp(c=c, req=req)
    runner = await op.acquire_cluster_lock()
    return c.start_op(op_name=OpName.restore, op=op, fun=runner)


@router.get("/list")
async def _list_backups(*, storage: Annotated[str, Body()] = "", c: Coordinator = Depends(), request: Request):
    req = ipc.ListRequest(storage=storage)
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
        list_response = await to_thread(list_backups, req=req, json_mstorage=c.json_mstorage)
        c.state.cached_list_response = CachedListResponse(
            coordinator_config=coordinator_config,
            list_request=req,
            list_response=msgspec.to_builtins(list_response),
        )
    finally:
        c.state.cached_list_running = False
    return msgspec.to_builtins(list_response)


@router.get("/delta/list")
async def _list_delta_backups(*, storage: Annotated[str, Body()] = "", c: Coordinator = Depends(), request: Request):
    req = ipc.ListRequest(storage=storage)
    # This is not supposed to be called very often, no caching necessary
    return await to_thread(list_delta_backups, req=req, json_mstorage=c.json_mstorage)


@router.post("/cleanup")
async def cleanup(
    *,
    storage: Annotated[str, Body()] = "",
    retention: Annotated[Optional[ipc.Retention], Body()] = None,
    explicit_delete: Annotated[Sequence[str], Body()] = (),
    c: Coordinator = Depends(),
):
    req = ipc.CleanupRequest(storage=storage, retention=retention, explicit_delete=list(explicit_delete))
    op = CleanupOp(c=c, req=req)
    runner = await op.acquire_cluster_lock()
    return c.start_op(op_name=OpName.cleanup, op=op, fun=runner)


@router.get("/{op_name}/{op_id}")
@router.get("/delta/{op_name}/{op_id}")
def op_status(*, op_name: OpName, op_id: int, c: Coordinator = Depends()):
    op, op_info = c.get_op_and_op_info(op_id=op_id, op_name=op_name)
    result = {"state": op_info.op_status}
    if isinstance(op, (BackupOp, DeltaBackupOp, RestoreOp)):
        result["progress"] = msgspec.to_builtins(op.progress)
    return result


@router.put("/{op_name}/{op_id}/sub-result")
@router.put("/delta/{op_name}/{op_id}/sub-result")
async def op_sub_result(*, op_name: OpName, op_id: int, c: Coordinator = Depends()):
    op, _ = c.get_op_and_op_info(op_id=op_id, op_name=op_name)
    # Someday, we might want to actually store results. This is sort
    # of spoofable endpoint though, so just triggering subsequent
    # result fetching faster. In case of terminal results, this
    # results only in one extra fetch per node, so not big deal.
    if not op.subresult_sleeper:
        return
    op.subresult_sleeper.wakeup()


@router.get("/busy")
async def is_busy(*, c: Coordinator = Depends()) -> bool:
    return c.is_busy()
