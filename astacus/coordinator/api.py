"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from .cleanup import CleanupOp
from .coordinator import Coordinator
from .list import list_backups
from .lockops import LockOps
from .plugins import get_plugin_backup_class, get_plugin_restore_class
from astacus.common import ipc
from astacus.common.op import Op
from enum import Enum
from fastapi import APIRouter, Depends
from urllib.parse import urljoin

import logging

router = APIRouter()

logger = logging.getLogger(__name__)


class OpName(str, Enum):
    """ (Long-running) operations defined in this API (for coordinator) """
    backup = "backup"
    lock = "lock"
    restore = "restore"
    unlock = "unlock"
    cleanup = "cleanup"


@router.get("/{op_name}/{op_id}")
def op_status(*, op_name: OpName, op_id: int, c: Coordinator = Depends()):
    _, op_info = c.get_op_and_op_info(op_id=op_id, op_name=op_name)
    return {"state": op_info.op_status}


class LockStartResult(Op.StartResult):
    unlock_url: str


@router.get("/")
def root():
    # Root is no-op, just useful for testing that Astacus is actually running
    return {}


@router.post("/lock")
async def lock(*, locker: str, ttl: int = 60, c: Coordinator = Depends()):
    op = LockOps(c=c, ttl=ttl, locker=locker)
    result = c.start_op(op_name=OpName.lock, op=op, fun=op.lock)
    return LockStartResult(unlock_url=urljoin(str(c.request.url), f"../unlock?locker={locker}"), **result.dict())


@router.post("/unlock")
def unlock(*, locker: str, c: Coordinator = Depends()):
    op = LockOps(c=c, locker=locker)
    return c.start_op(op_name=OpName.unlock, op=op, fun=op.unlock)


@router.post("/backup")
def backup(*, c: Coordinator = Depends()):
    op_class = get_plugin_backup_class(c.config.plugin)
    op = op_class(c=c)
    return c.start_op(op_name=OpName.backup, op=op, fun=op.run)


@router.post("/restore")
def restore(*, req: ipc.RestoreRequest = ipc.RestoreRequest(), c: Coordinator = Depends()):
    op_class = get_plugin_restore_class(c.config.plugin)
    op = op_class(c=c, req=req)
    return c.start_op(op_name=OpName.restore, op=op, fun=op.run)


@router.get("/list")
def _list_backups(*, req: ipc.ListRequest = ipc.ListRequest(), c: Coordinator = Depends()):
    return list_backups(req=req, json_mstorage=c.json_mstorage)


@router.post("/cleanup")
def cleanup(*, req: ipc.CleanupRequest = ipc.CleanupRequest(), c: Coordinator = Depends()):
    op = CleanupOp(c=c, req=req)
    return c.start_op(op_name=OpName.cleanup, op=op, fun=op.run)
