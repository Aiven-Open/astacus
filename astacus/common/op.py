"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Utility class for background operations.

Notable things:

- status URL (that can be used to access very coarse state)
- ~state machine (of sorts)

"""

from .exceptions import ExpiredOperationException
from .utils import AstacusModel
from astacus.common import magic, statsd
from dataclasses import dataclass, field
from enum import Enum
from fastapi import HTTPException
from typing import Optional
from urllib.parse import urlunsplit

import inspect
import logging

logger = logging.getLogger(__name__)


class Op:
    class Status(Enum):
        starting = "starting"
        running = "running"
        fail = "fail"
        done = "done"

    class Info(AstacusModel):
        op_id: int = 0
        op_name: str = ""
        op_status: Optional["Op.Status"]

    class StartResult(AstacusModel):
        op_id: int
        status_url: str

    op_id = -1  # set in start_op
    stats: Optional[statsd.StatsClient] = None  # set in start_op

    def __init__(self, *, info: Info):
        self.info = info

    def set_status(self, state: Status, *, from_state: Optional[Status] = None) -> bool:
        assert self.op_id, "start_op() should be called before set_status()"
        if self.info.op_id != self.op_id:
            raise ExpiredOperationException("operation id mismatch")
        if from_state and from_state != self.info.op_status:
            return False
        if self.info.op_status == state:
            return False
        logger.debug("%r state %s -> %s", self, self.info.op_status, state)
        self.info.op_status = state
        return True

    def set_status_fail(self):
        self.set_status(self.Status.fail)


Op.Info.update_forward_refs()


@dataclass
class OpState:
    op_info: Op.Info = field(default_factory=Op.Info)
    op: Optional[Op] = None


class OpMixin:
    """
    Convenience mixin which provides for both asynchronous as well as
    synchronous op starting functionality, and active job querying
    """
    def start_op(self, *, op, op_name, fun):
        info = self.state.op_info
        info.op_id += 1
        info.op_name = op_name
        self.state.op = op
        op.op_id = info.op_id
        op.set_status(Op.Status.starting)
        op.stats = self.stats
        url = self.request.url
        status_url = urlunsplit((url.scheme, url.netloc, f"{url.path}/{op.op_id}", "", ""))

        async def _async_wrapper():
            try:
                op.set_status(Op.Status.running)
                await fun()
                op.set_status(Op.Status.done, from_state=Op.Status.running)
            except Exception as ex:  # pylint: disable=broad-except
                op.set_status_fail()
                logger.warning("Unexpected exception during async %s %s %r", op, fun, ex)
                raise

        def _sync_wrapper():
            try:
                op.set_status(Op.Status.running)
                fun()
                op.set_status(Op.Status.done, from_state=Op.Status.running)
            except Exception as ex:  # pylint: disable=broad-except
                op.set_status_fail()
                logger.warning("Unexpected exception during sync %s %s %r", op, fun, ex)
                raise

        if inspect.iscoroutinefunction(fun):
            self.background_tasks.add_task(_async_wrapper)
        else:
            self.background_tasks.add_task(_sync_wrapper)

        return Op.StartResult(op_id=op.op_id, status_url=status_url)

    def get_op_and_op_info(self, *, op_id, op_name=None):
        op_info = self.state.op_info
        if op_id != op_info.op_id or (op_name and op_name != op_info.op_name):
            logger.info("request for nonexistent %s.%s != %r", op_name, op_id, op_info)
            raise HTTPException(
                404, {
                    "code": magic.ErrorCode.operation_id_mismatch,
                    "op": op_id,
                    "message": "Unknown operation id"
                }
            )
        return self.state.op, op_info
