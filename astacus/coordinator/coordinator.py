"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from .config import coordinator_config, CoordinatorConfig
from .state import coordinator_state, CoordinatorState
from astacus.common import utils
from enum import Enum
from fastapi import BackgroundTasks, Depends, Request
from typing import Optional
from urllib.parse import urljoin

import asyncio
import logging


class ExpiredOperationException(Exception):
    pass


logger = logging.getLogger(__name__)


class Op:
    class State(Enum):
        starting = "starting"
        running = "running"
        fail = "fail"
        done = "done"

    def __init__(self, *, c: "Coordinator"):
        self.state = c.state
        self.nodes = c.config.nodes[:]
        self.op = None

    def set_op_state(self,
                     state: State,
                     *,
                     from_state: Optional[State] = None):
        assert self.op, "start_op() should be called before set_op_state()"
        if self.state.op != self.op:
            raise ExpiredOperationException("operation id mismatch")
        if from_state and from_state != self.state.op_state:
            return
        logger.debug("%r state %s -> %s", self, self.state.op_state, state)
        self.state.op_state = state

    async def request_from_nodes(self, url, *, caller):
        urls = [f"{node.url}/{url}" for node in self.nodes]
        aws = [utils.httpx_request(url, caller=caller) for url in urls]
        return await asyncio.gather(*aws, return_exceptions=True)


class Coordinator:
    """ Convenience dependency which contains sub-dependencies most API endpoints need """
    def __init__(self,
                 *,
                 request: Request,
                 background_tasks: BackgroundTasks,
                 config: CoordinatorConfig = Depends(coordinator_config),
                 state: CoordinatorState = Depends(coordinator_state)):
        self.request = request
        self.background_tasks = background_tasks
        self.config = config
        self.state = state

    def start_op(self, *, op, fun):
        self.state.op += 1
        op.op = self.state.op
        op.set_op_state(Op.State.starting)
        status_url = urljoin(str(self.request.url),
                             f"../status/{self.state.op}")

        async def _wrapper():
            try:
                op.set_op_state(Op.State.running)
                await fun()
                op.set_op_state(Op.State.done, from_state=Op.State.running)
            except Exception as ex:  # pylint: disable=broad-except
                op.set_op_state(Op.State.fail)
                logger.warning("Unexpected exception during %s %s %r", op, fun,
                               ex)
                raise

        self.background_tasks.add_task(_wrapper)
        return {"status-url": status_url}
