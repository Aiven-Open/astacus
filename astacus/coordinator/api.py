"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from .cleanup import CleanupOp
from .coordinator import BackupOp, Coordinator, RestoreOp
from .list import list_backups
from .lockops import LockOps
from .state import CachedListResponse
from astacus import config
from astacus.common import ipc
from astacus.common.op import Op
from asyncio import to_thread
from enum import Enum
from fastapi import APIRouter, Depends, HTTPException, Request
from urllib.parse import urljoin

import logging
import os
import time

router = APIRouter()

logger = logging.getLogger(__name__)


class OpName(str, Enum):
    """(Long-running) operations defined in this API (for coordinator)"""

    backup = "backup"
    lock = "lock"
    restore = "restore"
    unlock = "unlock"
    cleanup = "cleanup"


@router.get("/{op_name}/{op_id}")
def op_status(*, op_name: OpName, op_id: int, c: Coordinator = Depends()):
    op, op_info = c.get_op_and_op_info(op_id=op_id, op_name=op_name)
    result = {"state": op_info.op_status}
    if isinstance(op, (BackupOp, RestoreOp)):
        result["progress"] = op.progress
    return result


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


@router.post("/lock")
async def lock(*, locker: str, c: Coordinator = Depends(), op: LockOps = Depends()):
    result = c.start_op(op_name=OpName.lock, op=op, fun=op.lock)
    return LockStartResult(unlock_url=urljoin(str(c.request_url), f"../unlock?locker={locker}"), **result.dict())


@router.post("/unlock")
def unlock(*, locker: str, c: Coordinator = Depends(), op: LockOps = Depends()):
    return c.start_op(op_name=OpName.unlock, op=op, fun=op.unlock)


@router.post("/backup")
async def backup(*, c: Coordinator = Depends(), op: BackupOp = Depends(BackupOp.create)):
    runner = await op.acquire_cluster_lock()
    return c.start_op(op_name=OpName.backup, op=op, fun=runner)


@router.post("/restore")
async def restore(*, c: Coordinator = Depends(), op: RestoreOp = Depends(RestoreOp.create)):
    runner = await op.acquire_cluster_lock()
    return c.start_op(op_name=OpName.restore, op=op, fun=runner)


@router.get("/list")
async def _list_backups(*, req: ipc.ListRequest = ipc.ListRequest(), c: Coordinator = Depends(), request: Request):
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
    list_response = await to_thread(list_backups, req=req, json_mstorage=c.json_mstorage)
    c.state.cached_list_response = CachedListResponse(
        coordinator_config=coordinator_config,
        list_request=req,
        list_response=list_response,
    )
    c.state.cached_list_running = False
    return list_response


@router.post("/cleanup")
async def cleanup(*, op: CleanupOp = Depends(), c: Coordinator = Depends()):
    runner = await op.acquire_cluster_lock()
    return c.start_op(op_name=OpName.cleanup, op=op, fun=runner)


@router.put("/{op_name}/{op_id}/sub-result")
async def op_sub_result(*, op_name: OpName, op_id: int, c: Coordinator = Depends()):
    op, _ = c.get_op_and_op_info(op_id=op_id, op_name=op_name)
    # Someday, we might want to actually store results. This is sort
    # of spoofable endpoint though, so just triggering subsequent
    # result fetching faster. In case of terminal results, this
    # results only in one extra fetch per node, so not big deal.
    if not op.subresult_sleeper:
        return
    op.subresult_sleeper.wakeup()
