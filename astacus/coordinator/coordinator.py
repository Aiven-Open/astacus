"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""
from .plugins.base import CoordinatorPlugin, OperationContext, Step, StepFailedError, StepsContext
from astacus.common import asyncstorage, exceptions, ipc, op, statsd, utils
from astacus.common.cachingjsonstorage import MultiCachingJsonStorage
from astacus.common.dependencies import get_request_url
from astacus.common.magic import ErrorCode
from astacus.common.progress import Progress
from astacus.common.rohmustorage import MultiRohmuStorage
from astacus.common.statsd import StatsClient
from astacus.common.storage import JsonStorage, MultiFileStorage, MultiStorage
from astacus.common.utils import AsyncSleeper
from astacus.coordinator.cluster import Cluster, LockResult, WaitResultError
from astacus.coordinator.config import coordinator_config, CoordinatorConfig, CoordinatorNode
from astacus.coordinator.plugins import PLUGINS
from astacus.coordinator.state import coordinator_state, CoordinatorState
from fastapi import BackgroundTasks, Depends, HTTPException, Request
from starlette.datastructures import URL
from typing import Any, Awaitable, Callable, Dict, Iterator, List, Optional, Type
from urllib.parse import urlunsplit

import asyncio
import contextlib
import logging
import socket
import threading
import time

logger = logging.getLogger(__name__)


def coordinator_stats(config: CoordinatorConfig = Depends(coordinator_config)) -> StatsClient:
    return StatsClient(config=config.statsd)


def coordinator_lock(request: Request) -> threading.RLock:
    return utils.get_or_create_state(state=request.app.state, key="sync_lock", factory=threading.RLock)


def coordinator_hexdigest_mstorage(config: CoordinatorConfig = Depends(coordinator_config)) -> MultiStorage:
    assert config.object_storage
    return MultiRohmuStorage(config=config.object_storage)


def coordinator_json_mstorage(config: CoordinatorConfig = Depends(coordinator_config)) -> MultiStorage:
    assert config.object_storage
    mstorage = MultiRohmuStorage(config=config.object_storage)
    if config.object_storage_cache:
        file_mstorage = MultiFileStorage(config.object_storage_cache)
        return MultiCachingJsonStorage(backend_mstorage=mstorage, cache_mstorage=file_mstorage)
    return mstorage


class Coordinator(op.OpMixin):
    state: CoordinatorState
    """ Convenience dependency which contains sub-dependencies most API endpoints need """

    def __init__(
        self,
        *,
        request_url: URL = Depends(get_request_url),
        background_tasks: BackgroundTasks,
        config: CoordinatorConfig = Depends(coordinator_config),
        state: CoordinatorState = Depends(coordinator_state),
        stats: statsd.StatsClient = Depends(coordinator_stats),
        sync_lock: threading.RLock = Depends(coordinator_lock),
        hexdigest_mstorage: MultiStorage = Depends(coordinator_hexdigest_mstorage),
        json_mstorage: MultiStorage = Depends(coordinator_json_mstorage),
    ):
        self.request_url = request_url
        self.background_tasks = background_tasks
        self.config = config
        self.state = state
        self.stats = stats
        self.sync_lock = sync_lock

        self.hexdigest_mstorage = hexdigest_mstorage
        self.json_mstorage = json_mstorage

    def get_operation_context(self, *, requested_storage: str = "") -> OperationContext:
        storage_name = self.get_storage_name(requested_storage=requested_storage)
        return OperationContext(
            storage_name=storage_name,
            json_storage=self.get_json_storage(storage_name),
            hexdigest_storage=self.get_hexdigest_storage(storage_name),
        )

    def get_plugin(self) -> CoordinatorPlugin:
        return PLUGINS[self.config.plugin].parse_obj(self.config.plugin_config)

    def get_storage_name(self, *, requested_storage: str = ""):
        return requested_storage if requested_storage else self.json_mstorage.get_default_storage_name()

    def get_hexdigest_storage(self, storage_name: str) -> asyncstorage.AsyncHexDigestStorage:
        return asyncstorage.AsyncHexDigestStorage(self.hexdigest_mstorage.get_storage(storage_name))

    def get_json_storage(self, storage_name: str) -> asyncstorage.AsyncJsonStorage:
        storage = CacheClearingJsonStorage(state=self.state, storage=self.json_mstorage.get_storage(storage_name))
        return asyncstorage.AsyncJsonStorage(storage)


class CacheClearingJsonStorage(JsonStorage):
    def __init__(self, state: CoordinatorState, storage: JsonStorage):
        self.state = state
        self.storage = storage

    def delete_json(self, name: str) -> None:
        try:
            return self.storage.delete_json(name)
        finally:
            self.state.cached_list_response = None

    def download_json(self, name: str) -> Dict[str, Any]:
        return self.storage.download_json(name)

    def list_jsons(self) -> List[str]:
        return self.storage.list_jsons()

    def upload_json_str(self, name: str, data: str) -> None:
        try:
            return self.storage.upload_json_str(name, data)
        finally:
            self.state.cached_list_response = None


class CoordinatorOp(op.Op):
    def __init__(self, *, c: Coordinator, op_id: int, stats: StatsClient):
        super().__init__(info=c.state.op_info, op_id=op_id, stats=stats)
        self.request_url = c.request_url
        self.nodes = c.config.nodes
        self.poll_config = c.config.poll
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


class LockedCoordinatorOp(CoordinatorOp):
    op_started: Optional[float]  # set when op_info.status is set to starting

    def __init__(self, *, c: Coordinator, op_id: int, stats: StatsClient):
        super().__init__(c=c, op_id=op_id, stats=stats)
        self.ttl = c.config.default_lock_ttl
        self.initial_lock_start = time.monotonic()
        self.locker = self.get_locker()

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
                409,
                {
                    "code": ErrorCode.cluster_lock_unavailable,
                    "message": "Unable to acquire cluster lock to create operation",
                },
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


class SteppedCoordinatorOp(LockedCoordinatorOp):
    attempts: int
    steps: List[Step]
    step_progress: Dict[Type[Step], Progress]

    def __init__(self, *, c: Coordinator, op_id: int, stats: StatsClient, attempts: int, steps: List[Step]):
        super().__init__(c=c, op_id=op_id, stats=stats)
        self.state = c.state
        self.attempts = attempts
        self.steps = steps
        self.step_progress = {}

    @property
    def progress(self) -> Progress:
        return Progress.merge(self.step_progress.values())

    async def run_with_lock(self, cluster: Cluster) -> None:
        name = self.__class__.__name__
        try:
            for attempt in range(1, self.attempts + 1):
                logger.debug("%s - attempt #%d/%d", name, attempt, self.attempts)
                context = StepsContext(attempt=attempt)
                stats_tags = {"op": name, "attempt": str(attempt)}
                async with self.stats.async_timing_manager("astacus_attempt_duration", stats_tags):
                    try:
                        if await self.try_run(cluster, context):
                            return
                    except exceptions.TransientException as ex:
                        logger.info("%s - transient failure: %r", name, ex)
        except exceptions.PermanentException as ex:
            logger.info("%s - permanent failure: %r", name, ex)
        self.set_status_fail()

    async def try_run(self, cluster: Cluster, context: StepsContext) -> bool:
        op_name = self.__class__.__name__
        for i, step in enumerate(self.steps, 1):
            step_name = step.__class__.__name__
            if self.state.shutting_down:
                logger.info("Step %s not even started due to shutdown", step_name)
                return False
            logger.debug("Step %d/%d: %s", i, len(self.steps), step_name)
            async with self.stats.async_timing_manager("astacus_step_duration", {"op": op_name, "step": step_name}):
                with self._progress_handler(cluster, step):
                    try:
                        r = await step.run_step(cluster, context)
                    except (StepFailedError, WaitResultError) as e:
                        logger.info("Step %s failed: %s", step, str(e))
                        return False
            context.set_result(step.__class__, r)
        return True

    @contextlib.contextmanager
    def _progress_handler(self, cluster: Cluster, step: Step) -> Iterator[None]:
        def progress_handler(progress: Progress):
            self.step_progress[step.__class__] = progress

        cluster.set_progress_handler(progress_handler)
        try:
            yield
        finally:
            cluster.set_progress_handler(None)


class BackupOp(SteppedCoordinatorOp):
    def __init__(self, *, c: Coordinator, op_id: int, stats: StatsClient):
        context = c.get_operation_context()
        steps = c.get_plugin().get_backup_steps(context=context)
        super().__init__(c=c, op_id=op_id, stats=stats, attempts=c.config.backup_attempts, steps=steps)


class RestoreOp(SteppedCoordinatorOp):
    def __init__(self, *, c: Coordinator, op_id: int, stats: StatsClient, req: ipc.RestoreRequest):
        context = c.get_operation_context(requested_storage=req.storage)
        steps = c.get_plugin().get_restore_steps(context=context, req=req)
        if req.stop_after_step is not None:
            step_names = [step.__class__.__name__ for step in steps]
            step_index = step_names.index(req.stop_after_step)
            steps = steps[: step_index + 1]
        super().__init__(c=c, op_id=op_id, stats=stats, attempts=1, steps=steps)  # c.config.restore_attempts
