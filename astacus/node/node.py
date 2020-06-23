"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

"""

from .config import node_config, NodeConfig
from .snapshotter import Snapshotter
from .state import node_state, NodeState
from astacus.common import ipc, magic, op, statsd, utils
from astacus.common.rohmustorage import RohmuStorage
from fastapi import BackgroundTasks, Depends, Request
from typing import Optional

import logging

logger = logging.getLogger(__name__)
SNAPSHOTTER_KEY = "node_snapshotter"


class NodeOp(op.Op):
    req: Optional[ipc.NodeRequest] = None  # Provided by subclass

    def __init__(self, *, n: "Node"):
        super().__init__(info=n.state.op_info)
        self.start_op = n.start_op
        self.config = n.config
        self._still_locked_callback = n.state.still_locked_callback
        self._sent_result_json = None
        self.result = self.create_result()
        self.result.az = self.config.az
        self.get_or_create_snapshotter = n.get_or_create_snapshotter
        self.get_snapshotter = n.get_snapshotter
        # TBD: Could start some worker thread to send the self.result periodically
        # (or to some local start method )

    def create_result(self):
        return ipc.NodeResult()

    @property
    def storage(self):
        return RohmuStorage(self.config.object_storage, storage=self.req.storage)

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
        result_json = self.result.json(exclude_defaults=True)
        if result_json == self._sent_result_json:
            return
        self._sent_result_json = result_json
        utils.http_request(self.req.result_url, method="put", caller="NodeOp.send_result", data=result_json)

    def set_status(self, state: op.Op.Status, *, from_state: Optional[op.Op.Status] = None) -> bool:
        if not super().set_status(state, from_state=from_state):
            # State didn't change, do nothing
            return False
        if state == self.Status.fail:
            progress = self.result.progress
            if not progress.final:
                progress.add_fail()
                progress.done()
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
        self.stats = statsd.StatsClient(config=config.statsd)

    def get_or_create_snapshotter(self, root_globs):
        root_link = self.config.root_link
        if not root_link:
            root_link = self.config.root / magic.ASTACUS_TMPDIR
        root_link.mkdir(exist_ok=True)

        def _create_snapshotter():
            return Snapshotter(src=self.config.root, dst=root_link, globs=root_globs, parallel=self.config.parallel.hashes)

        return utils.get_or_create_state(request=self.request, key=SNAPSHOTTER_KEY, factory=_create_snapshotter)

    def get_snapshotter(self):
        return getattr(self.request.app.state, SNAPSHOTTER_KEY)
