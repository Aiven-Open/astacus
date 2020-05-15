"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from .backup import BackupOp
from .coordinator import Coordinator
from .lockops import LockOps
from astacus.common.op import Op
from enum import Enum
from fastapi import APIRouter, Depends
from urllib.parse import urljoin

import logging

router = APIRouter()

logger = logging.getLogger(__name__)


class ErrorCode(str, Enum):
    operation_id_mismatch = "operation_id_mismatch"


class OpName(str, Enum):
    lock = "lock"
    unlock = "unlock"
    backup = "backup"


@router.get("/{op_name}/{op_id}")
def status(*, op_name: OpName, op_id: int, c: Coordinator = Depends()):
    _, op_info = c.get_op_and_op_info(op_id=op_id, op_name=op_name)
    return {"state": op_info.op_status}


class LockStartResult(Op.StartResult):
    unlock_url: str


@router.post("/lock")
async def lock(*, locker: str, ttl: int = 60, c: Coordinator = Depends()):
    op = LockOps(c=c, ttl=ttl, locker=locker)
    result = c.start_op(op_name="lock", op=op, fun=op.lock)
    return LockStartResult(unlock_url=urljoin(str(c.request.url), f"../unlock?locker={locker}"), **result.dict())


@router.post("/unlock")
def unlock(*, locker: str, c: Coordinator = Depends()):
    op = LockOps(c=c, locker=locker)
    return c.start_op(op_name="unlock", op=op, fun=op.unlock)


@router.post("/backup")
def backup(*, c: Coordinator = Depends()):
    op = BackupOp(c=c)
    return c.start_op(op_name="backup", op=op, fun=op.run)
