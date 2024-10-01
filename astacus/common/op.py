"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Utility class for background operations.

Notable things:

- status URL (that can be used to access very coarse state)
- ~state machine (of sorts)

"""

from . import magic
from .exceptions import ExpiredOperationException
from .statsd import StatsClient
from .utils import AstacusModel
from astacus.starlette import json_http_exception
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from starlette.background import BackgroundTasks
from starlette.datastructures import URL
from typing import Any, Optional
from urllib.parse import urlunsplit

import asyncio
import contextlib
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
        op_status: Optional["Op.Status"] = None

    class StartResult(AstacusModel):
        op_id: int
        status_url: str

    op_id: int
    stats: StatsClient

    def __init__(self, *, info: Info, op_id: int, stats: StatsClient):
        self.info = info
        self.op_id = op_id
        self.stats = stats

    def check_op_id(self):
        if self.info.op_id != self.op_id:
            raise ExpiredOperationException("operation id mismatch")

    def set_status(self, status: Status, *, from_status: Status | None = None) -> bool:
        assert self.op_id, "start_op() should be called before set_status()"
        self.check_op_id()
        if from_status and from_status != self.info.op_status:
            return False
        if self.info.op_status == status:
            return False
        logger.info("%s.%d status %s -> %s", self.info.op_name, self.info.op_id, self.info.op_status, status)
        self.info.op_status = status
        return True

    def set_status_fail(self):
        name = self.__class__.__name__
        self.stats.increase("astacus_fail", tags={"op": name})
        self.set_status(self.Status.fail)


Op.Info.update_forward_refs()


@dataclass
class OpState:
    op_info: Op.Info = field(default_factory=Op.Info)
    op: Op | None = None
    next_op_id: int = 1


class OpMixin:
    """
    Convenience mixin which provides for both asynchronous as well as
    synchronous op starting functionality, and active job querying
    """

    state: OpState
    stats: StatsClient
    request_url: URL
    background_tasks: BackgroundTasks

    def allocate_op_id(self) -> int:
        try:
            return self.state.next_op_id
        finally:
            self.state.next_op_id += 1

    def start_op(self, *, op: Op, op_name: str, fun: Callable[[], Any]) -> Op.StartResult:
        info = self.state.op_info
        info.op_id = op.op_id
        info.op_name = op_name
        self.state.op = op
        op.set_status(Op.Status.starting)
        url = self.request_url
        status_url = urlunsplit((url.scheme, url.netloc, f"{url.path}/{op.op_id}", "", ""))

        async def _async_wrapper():
            print("runs")
            try:
                op.set_status(Op.Status.running)
                await fun()
                op.set_status(Op.Status.done, from_status=Op.Status.running)
            except ExpiredOperationException:
                pass
            except asyncio.CancelledError:
                with contextlib.suppress(ExpiredOperationException):
                    op.set_status_fail()
            except Exception as ex:  # pylint: disable=broad-except
                logger.warning("Unexpected exception during async %s %s %r", op, fun, ex)
                with contextlib.suppress(ExpiredOperationException):
                    op.set_status_fail()
                raise

        def _sync_wrapper():
            try:
                op.set_status(Op.Status.running)
                fun()
                op.set_status(Op.Status.done, from_status=Op.Status.running)
            except ExpiredOperationException:
                pass
            except Exception as ex:  # pylint: disable=broad-except
                logger.warning("Unexpected exception during sync %s %s %r", op, fun, ex)
                with contextlib.suppress(ExpiredOperationException):
                    op.set_status_fail()
                raise

        if inspect.iscoroutinefunction(fun):
            self.background_tasks.add_task(_async_wrapper)
        else:
            self.background_tasks.add_task(_sync_wrapper)

        return Op.StartResult(op_id=op.op_id, status_url=status_url)

    def get_op_and_op_info(self, *, op_id: int, op_name: str | None = None):
        op_info = self.state.op_info
        if op_id != op_info.op_id or (op_name and op_name != op_info.op_name):
            logger.info("request for nonexistent %s.%s != %r", op_name, op_id, op_info)
            raise json_http_exception(
                404,
                {
                    "code": magic.ErrorCode.operation_id_mismatch,
                    "op": op_id,
                    "message": "Unknown operation id",
                },
            )
        return self.state.op, op_info
