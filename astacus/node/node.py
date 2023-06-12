"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

"""

from .config import node_config, NodeConfig
from .snapshotter import Snapshotter
from .state import node_state, NodeState
from astacus.common import ipc, magic, op, statsd, utils
from astacus.common.dependencies import get_request_app_state, get_request_url
from astacus.common.snapshot import SnapshotGroup
from astacus.common.statsd import StatsClient
from fastapi import BackgroundTasks, Depends
from starlette.datastructures import URL
from typing import Generic, Optional, Sequence, TypeVar

import logging

logger = logging.getLogger(__name__)
SNAPSHOTTER_KEY = "node_snapshotter"

Request = TypeVar("Request", bound=ipc.NodeRequest)
Result = TypeVar("Result", bound=ipc.NodeResult)


class NodeOp(op.Op, Generic[Request, Result]):
    def __init__(self, *, n: "Node", op_id: int, req: Request, stats: StatsClient) -> None:
        super().__init__(info=n.state.op_info, op_id=op_id, stats=stats)
        self.start_op = n.start_op
        self.config = n.config
        self._still_locked_callback = n.state.still_locked_callback
        self._sent_result_json: Optional[str] = None
        self.req = req
        self.result = self.create_result()
        self.result.az = self.config.az
        self.get_or_create_snapshotter = n.get_or_create_snapshotter
        self.get_snapshotter = n.get_snapshotter
        # TBD: Could start some worker thread to send the self.result periodically
        # (or to some local start method )

    def create_result(self) -> Result:
        raise NotImplementedError

    def still_running_callback(self) -> bool:
        if self.info.op_id != self.op_id:
            return False
        return self._still_locked_callback()

    def send_result(self) -> None:
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

    def set_status(self, status: op.Op.Status, *, from_status: Optional[op.Op.Status] = None) -> bool:
        if not super().set_status(status, from_status=from_status):
            # Status didn't change, do nothing
            return False
        if status == self.Status.fail:
            progress = self.result.progress
            if not progress.final:
                progress.add_fail()
                progress.done()
        elif status != self.Status.done:
            return True
        # fail or done; either way, we're done, send final result
        self.send_result()
        return True


def node_stats(config: NodeConfig = Depends(node_config)) -> statsd.StatsClient:
    return statsd.StatsClient(config=config.statsd)


class Node(op.OpMixin):
    state: NodeState
    """ Convenience dependency which contains sub-dependencies most API endpoints need """

    def __init__(
        self,
        *,
        app_state: object = Depends(get_request_app_state),
        request_url: URL = Depends(get_request_url),
        background_tasks: BackgroundTasks,
        config: NodeConfig = Depends(node_config),
        state: NodeState = Depends(node_state),
        stats: statsd.StatsClient = Depends(node_stats)
    ) -> None:
        self.app_state = app_state
        self.request_url = request_url
        self.background_tasks = background_tasks
        self.config = config
        self.state = state
        self.stats = stats

    def get_or_create_snapshotter(self, groups: Sequence[SnapshotGroup]) -> Snapshotter:
        root_link = self.config.root_link if self.config.root_link is not None else self.config.root / magic.ASTACUS_TMPDIR
        root_link.mkdir(exist_ok=True)

        def _create_snapshotter() -> Snapshotter:
            return Snapshotter(src=self.config.root, dst=root_link, groups=groups, parallel=self.config.parallel.hashes)

        return utils.get_or_create_state(state=self.app_state, key=SNAPSHOTTER_KEY, factory=_create_snapshotter)

    def get_snapshotter(self) -> Snapshotter:
        return getattr(self.app_state, SNAPSHOTTER_KEY)
