"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

"""

from .config import node_config, NodeConfig
from .state import node_state, NodeState
from astacus.common import ipc, op, utils
from astacus.common.progress import Progress
from astacus.common.rohmuhashstorage import RohmuHashStorage
from astacus.common.utils import AstacusModel
from fastapi import BackgroundTasks, Depends, Request
from typing import Optional

import logging

logger = logging.getLogger(__name__)


class NodeOp(op.Op):
    req: Optional[ipc.NodeRequest] = None  # Provided by subclass
    result: Optional[AstacusModel] = None  # Provided by subclass
    progress: Optional[Progress] = None  # Provided by subclass

    def __init__(self, *, n: "Node"):
        super().__init__(info=n.state.op_info)
        self.start_op = n.start_op
        self.config = n.config
        self._still_locked_callback = n.state.still_locked_callback
        self._sent_result_json = None

    @property
    def storage(self):
        return RohmuHashStorage(self.config.object_storage)

    def still_running_callback(self):
        if self.info.op_id != self.op_id:
            return False
        return self._still_locked_callback()

    def send_result(self):
        assert self.req and self.result, "subclass responsibility to set up req/result before send_result"
        if not self.req.result_url:
            logger.debug("send_result omitted - no result_url")
            return
        if not self.still_running_callback():
            logger.debug("send_result omitted - not running")
            return
        result_json = self.result.json(exclude_unset=True)
        if result_json == self._sent_result_json:
            return
        self._sent_result_json = result_json
        utils.http_request(self.req.result_url, method="put", caller="SnapshotOps.snapshot", data=result_json)

    def set_status(self, state: op.Op.Status, *, from_state: Optional[op.Op.Status] = None) -> bool:
        if not super().set_status(state, from_state=from_state):
            # State didn't change, do nothing
            return False
        if state == self.Status.fail:
            if self.progress and not self.progress.final and not self.progress.failed:
                self.progress.add_fail()
        elif state != self.Status.done:
            return True
        # fail or done; either way, we're done, send final result
        self.send_result()
        return True


class Node(op.OpMixin):
    """ Convenience dependency which contains sub-dependencies most API endpoints need """
    def __init__(
        self,
        *,
        request: Request,
        background_tasks: BackgroundTasks,
        config: NodeConfig = Depends(node_config),
        state: NodeState = Depends(node_state)
    ):
        self.request = request
        self.background_tasks = background_tasks
        self.config = config
        self.state = state
