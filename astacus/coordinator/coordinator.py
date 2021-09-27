"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""
from astacus.common import asyncstorage, magic, op, statsd, utils
from astacus.common.cachingjsonstorage import MultiCachingJsonStorage
from astacus.common.progress import Progress
from astacus.common.rohmustorage import MultiRohmuStorage
from astacus.common.storage import MultiFileStorage, MultiStorage
from astacus.common.utils import AsyncSleeper
from astacus.coordinator.cluster import Cluster, LockResult
from astacus.coordinator.config import coordinator_config, CoordinatorConfig, CoordinatorNode
from astacus.coordinator.state import coordinator_state, CoordinatorState
from datetime import datetime
from fastapi import BackgroundTasks, Depends, HTTPException, Request
from starlette.datastructures import URL
from typing import Awaitable, Callable, List, Optional
from urllib.parse import urlunsplit

import asyncio
import logging
import socket
import threading
import time

logger = logging.getLogger(__name__)


class CoordinatorOp(op.Op):
    attempt = -1  # try_run iteration number
    attempt_start: Optional[datetime] = None  # try_run iteration start time

    def __init__(self, *, c: "Coordinator", op_id: int):
        super().__init__(info=c.state.op_info, op_id=op_id)
        self.request_url = c.request_url
        self.nodes = c.config.nodes
        self.poll_config = c.config.poll
        self.request_url = c.request.url
        self.config = c.config
        self.state = c.state
        self.hexdigest_mstorage = c.hexdigest_mstorage
        self.json_mstorage = c.json_mstorage
        self.set_storage_name(self.default_storage_name)
        self.subresult_sleeper = AsyncSleeper()

    def get_cluster(self) -> Cluster:
        # The only reason this exists is because op_id and stats are added after the op is created
        return Cluster(
            nodes=self.nodes,
            poll_config=self.poll_config,
            subresult_url=get_subresult_url(self.request_url, self.op_id),
            subresult_sleeper=self.subresult_sleeper,
            stats=self.stats,
        )

    hexdigest_storage: Optional[asyncstorage.AsyncHexDigestStorage] = None
    json_storage: Optional[asyncstorage.AsyncJsonStorage] = None

    def set_storage_name(self, storage_name):
        self.hexdigest_storage = asyncstorage.AsyncHexDigestStorage(self.hexdigest_mstorage.get_storage(storage_name))
        self.json_storage = asyncstorage.AsyncJsonStorage(self.json_mstorage.get_storage(storage_name))

    @property
    def default_storage_name(self):
        return self.json_mstorage.get_default_storage_name()

    async def wait_successful_results(self, start_results, *, result_class, all_nodes=True):
        urls = []

        for i, result in enumerate(start_results, 1):
            if not result or isinstance(result, Exception):
                logger.info("wait_successful_results: Incorrect start result for #%d/%d: %r", i, len(start_results), result)
                return []
            parsed_result = op.Op.StartResult.parse_obj(result)
            urls.append(parsed_result.status_url)
        if all_nodes and len(urls) != len(self.nodes):
            return []
        delay = self.config.poll.delay_start
        results = [None] * len(urls)
        # Note that we don't have timeout mechanism here as such,
        # however, if re-locking times out, we will bail out. TBD if
        # we need timeout mechanism here anyway.
        failures = {}

        async for _ in utils.exponential_backoff(
            initial=delay,
            multiplier=self.config.poll.delay_multiplier,
            maximum=self.config.poll.delay_max,
            duration=self.config.poll.duration,
            async_sleeper=self.subresult_sleeper,
        ):
            for i, (url, result) in enumerate(zip(urls, results)):
                # TBD: This could be done in parallel too
                if result is not None and result.progress.final:
                    continue
                r = await utils.httpx_request(
                    url, caller="CoordinatorOp.wait_successful_results", timeout=self.config.poll.result_timeout
                )
                if r is None:
                    failures[i] = failures.get(i, 0) + 1
                    if failures[i] >= self.config.poll.maximum_failures:
                        return []
                    continue
                # We got something -> decode the result
                result = result_class.parse_obj(r)
                results[i] = result
                failures[i] = 0
                assert self.current_step
                self.step_progress[self.current_step] = Progress.merge(r.progress for r in results if r is not None)
                if result.progress.finished_failed:
                    return []
            if not any(True for result in results if result is None or not result.progress.final):
                break
        else:
            logger.debug("wait_successful_results timed out")
            return []
        return results


class CoordinatorOpWithClusterLock(CoordinatorOp):
    op_started: Optional[float]  # set when op_info.status is set to starting

    def __init__(self, *, c: "Coordinator", op_id: int):
        super().__init__(c=c, op_id=op_id)
        self.ttl = c.config.default_lock_ttl
        self.initial_lock_start = time.monotonic()
        self.locker = self.get_locker()
        self.relock_tasks: List[asyncio.Task] = []

    def get_locker(self):
        return f"{socket.gethostname()}-{id(self)}"

    async def run_with_lock(self, cluster: Cluster) -> None:
        raise NotImplementedError

    async def acquire_cluster_lock(self) -> Callable[[], Awaitable]:
        # Acquire initial locks
        cluster = self.get_cluster()
        result = await cluster.request_lock(locker=self.locker, ttl=self.ttl)
        if result is not LockResult.ok:
            # Ensure we don't wind up holding partial lock on the cluster
            await cluster.request_unlock(locker=self.locker)
            raise HTTPException(
                409, {
                    "code": magic.ErrorCode.cluster_lock_unavailable,
                    "message": "Unable to acquire cluster lock to create operation"
                }
            )

        async def run():
            relock_tasks = await self._create_relock_tasks(cluster)
            try:
                await self.run_with_lock(cluster)
            finally:
                for relock_task in relock_tasks:
                    relock_task.cancel()
                await asyncio.gather(*relock_tasks, return_exceptions=True)
                await cluster.request_unlock(locker=self.locker)

        return run

    async def _create_relock_tasks(self, cluster: Cluster) -> List[asyncio.Task]:
        current_task = asyncio.current_task()
        assert current_task is not None
        return [
            asyncio.create_task(self._node_relock_loop(main_task=current_task, cluster=cluster, node=node))
            for node in cluster.nodes
        ]

    async def _node_relock_loop(self, *, main_task: asyncio.Task, cluster: Cluster, node: CoordinatorNode) -> None:
        lock_eol = self.initial_lock_start + self.ttl
        next_lock = self.initial_lock_start + self.ttl / 2
        while True:
            self._update_running_stats()
            t = time.monotonic()
            if t > lock_eol:
                logger.info("Lock of node %r expired, canceling operation", node)
                main_task.cancel()
                return
            while t < next_lock:
                left = next_lock - t + 0.01
                logger.debug("_node_relock_loop sleeping %r", left)
                await asyncio.sleep(left)
                t = time.monotonic()

            # Attempt to reacquire lock
            r = await cluster.request_relock(node=node, locker=self.locker, ttl=self.ttl)
            if r == LockResult.ok:
                lock_eol = t + self.ttl
                next_lock = t + self.ttl / 2
            elif r == LockResult.failure:
                logger.info("Relock of node %r failed, canceling operation", node)
                main_task.cancel()
                return
            elif r == LockResult.exception:
                # We attempt ~4-5 times until giving up
                await asyncio.sleep(self.ttl / 10)
            else:
                raise NotImplementedError(f"Unknown result from request_lock_call_from_nodes:{r!r}")

    def set_status(self, status: op.Op.Status, *, from_status: Optional[op.Op.Status] = None) -> bool:
        changed = super().set_status(status=status, from_status=from_status)
        if status == op.Op.Status.starting and changed:
            self.op_started = utils.monotonic_time()
        self._update_running_stats()
        return changed

    def _update_running_stats(self) -> None:
        if self.stats is not None:
            if self.info.op_status in {op.Op.Status.done, op.Op.Status.fail}:
                op_running_for = 0
            else:
                op_running_for = int(utils.monotonic_time() - self.op_started)
            logger.debug("Sending op_running_for metric. value=%d", op_running_for)
            self.stats.gauge("astacus_op_running_for", op_running_for, tags={"op": self.info.op_name, "id": self.info.op_id})


def get_subresult_url(request_url: URL, op_id: int) -> str:
    url = request_url
    parts = [url.scheme, url.netloc, f"{url.path}/{op_id}/sub-result", "", ""]
    return urlunsplit(parts)


class Coordinator(op.OpMixin):
    """ Convenience dependency which contains sub-dependencies most API endpoints need """
    state: CoordinatorState

    def __init__(
        self,
        *,
        request: Request,
        background_tasks: BackgroundTasks,
        config: CoordinatorConfig = Depends(coordinator_config),
        state: CoordinatorState = Depends(coordinator_state)
    ):
        self.request = request
        self.request_url = request.url
        self.background_tasks = background_tasks
        self.config = config
        self.state = state
        self.stats = statsd.StatsClient(config=config.statsd)

        assert self.config.object_storage
        mstorage = MultiRohmuStorage(config=self.config.object_storage)
        self.hexdigest_mstorage = mstorage
        json_mstorage: MultiStorage = mstorage
        if self.config.object_storage_cache:
            file_mstorage = MultiFileStorage(self.config.object_storage_cache)
            json_mstorage = MultiCachingJsonStorage(backend_mstorage=mstorage, cache_mstorage=file_mstorage)
        self.json_mstorage = json_mstorage
        self.sync_lock = utils.get_or_create_state(state=request.app.state, key="sync_lock", factory=threading.RLock)
