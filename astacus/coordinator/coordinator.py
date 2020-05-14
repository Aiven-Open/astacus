"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from .config import coordinator_config, CoordinatorConfig
from .state import coordinator_state, CoordinatorState
from astacus.common import op, utils
from fastapi import BackgroundTasks, Depends, Request

import asyncio


class CoordinatorOp(op.Op):
    def __init__(self, *, c: "Coordinator"):
        super().__init__(info=c.state.op_info)
        self.nodes = c.config.nodes

    async def request_from_nodes(self, url, *, caller, method="get"):
        urls = [f"{node.url}/{url}" for node in self.nodes]
        aws = [
            utils.httpx_request(url, method=method, caller=caller)
            for url in urls
        ]
        return await asyncio.gather(*aws, return_exceptions=True)


class Coordinator(op.OpStartMixin):
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
