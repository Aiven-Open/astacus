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
from astacus.node.snapshot import Snapshot
from astacus.node.sqlite_snapshot import SQLiteSnapshot, SQLiteSnapshotter
from collections.abc import Sequence
from fastapi import BackgroundTasks, Depends
from pathlib import Path
from starlette.datastructures import URL
from typing import Generic, TypeVar

import logging
import msgspec

logger = logging.getLogger(__name__)
SNAPSHOTTER_KEY = "node_snapshotter"
DELTA_SNAPSHOTTER_KEY = "node_delta_snapshotter"

Request = TypeVar("Request", bound=ipc.NodeRequest)
Result = TypeVar("Result", bound=ipc.NodeResult)


class NodeOp(op.Op, Generic[Request, Result]):
    def __init__(self, *, n: "Node", op_id: int, req: Request, stats: StatsClient) -> None:
        super().__init__(info=n.state.op_info, op_id=op_id, stats=stats)
        self.start_op = n.start_op
        self.config = n.config
        self._still_locked_callback = n.state.still_locked_callback
        self.req = req
        self.result = self.create_result()
        self.result.az = self.config.az
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
        # We used to send the entire  json-encoded result but not use it, wasting a lot of memory for nothing
        status = msgspec.json.encode(
            ipc.NodeResult(
                hostname=self.result.hostname,
                az=self.result.az,
                progress=self.result.progress,
            )
        )
        utils.http_request(self.req.result_url, method="put", caller="NodeOp.send_result", data=status)

    def set_status(self, status: op.Op.Status, *, from_status: op.Op.Status | None = None) -> bool:
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
        stats: statsd.StatsClient = Depends(node_stats),
    ) -> None:
        self.app_state = app_state
        self.request_url = request_url
        self.background_tasks = background_tasks
        self.config = config
        self.state = state
        self.stats = stats

    def get_or_create_snapshot(self) -> Snapshot:
        return self._get_or_create_snapshot(SNAPSHOTTER_KEY, self.config.root_link)

    def get_or_create_delta_snapshot(self) -> Snapshot:
        return self._get_or_create_snapshot(DELTA_SNAPSHOTTER_KEY, self.config.delta_root_link)

    def _get_or_create_snapshot(self, key: str, path: Path | None) -> Snapshot:
        root_link = path if path is not None else self.config.root / magic.ASTACUS_TMPDIR

        def _create_snapshot() -> Snapshot:
            snapshotter_db_name = f"{key}.db"
            return SQLiteSnapshot(root_link, self.config.db_path / snapshotter_db_name)

        return utils.get_or_create_state(state=self.app_state, key=key, factory=_create_snapshot)

    def get_snapshotter(self, groups: Sequence[SnapshotGroup]) -> Snapshotter:
        return self._get_snapshotter_for_snapshot(self.get_or_create_snapshot(), groups)

    def get_delta_snapshotter(self, groups: Sequence[SnapshotGroup]) -> Snapshotter:
        return self._get_snapshotter_for_snapshot(self.get_or_create_delta_snapshot(), groups)

    def _get_snapshotter_for_snapshot(self, snapshot: Snapshot, groups: Sequence[SnapshotGroup]) -> Snapshotter:
        if isinstance(snapshot, SQLiteSnapshot):
            return SQLiteSnapshotter(groups, self.config.root, snapshot.dst, snapshot, self.config.parallel.hashes)
        raise NotImplementedError(f"Unknown snapshot type {type(snapshot)}")
