"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""

from .config import coordinator_config, CoordinatorConfig
from .state import coordinator_state, CoordinatorState
from astacus.common import magic, op, utils
from astacus.common.magic import LockCall
from astacus.common.rohmustorage import RohmuStorage
from enum import Enum
from fastapi import BackgroundTasks, Depends, Request
from starlette.concurrency import run_in_threadpool

import asyncio
import json
import logging
import socket
import time

logger = logging.getLogger(__name__)


class LockResult(Enum):
    ok = "ok"
    failure = "failure"
    exception = "exception"


class AsyncStorageWrapper:
    """Subset of the Storage API proxied async -> sync via starlette threadpool

    Note that the access is not intentionally locked; therefore even
    synchronous API can be used in parallel (at least if it is safe to
    do so) using this.

    """
    def __init__(self, storage):
        self.storage = storage

    async def delete_hexdigest(self, hexdigest: str):
        return await run_in_threadpool(self.storage.delete_hexdigest, hexdigest)

    async def delete_json(self, name: str):
        return await run_in_threadpool(self.storage.delete_json, name)

    async def download_json(self, name: str):
        return await run_in_threadpool(self.storage.download_json, name)

    async def list_hexdigests(self):
        return await run_in_threadpool(self.storage.list_hexdigests)

    async def list_jsons(self):
        return await run_in_threadpool(self.storage.list_jsons)

    async def upload_json(self, name: str, data):
        return await run_in_threadpool(self.storage.upload_json, name, data)


class CoordinatorOp(op.Op):
    def __init__(self, *, c: "Coordinator"):
        super().__init__(info=c.state.op_info)
        self.nodes = c.config.nodes
        self.request_url = c.request.url
        self.config = c.config

    @property
    def async_storage(self):
        return AsyncStorageWrapper(storage=self.storage)

    @property
    def storage(self):
        return RohmuStorage(self.config.object_storage)

    async def request_from_nodes(self, url, *, caller, nodes=None, **kw):
        if nodes is None:
            nodes = self.nodes
        urls = [f"{node.url}/{url}" for node in nodes]
        aws = [utils.httpx_request(url, caller=caller, **kw) for url in urls]
        results = await asyncio.gather(*aws, return_exceptions=True)
        logger.debug("request_from_nodes %r => %r", nodes, results)
        return results

    async def request_lock_call_from_nodes(self, *, call: LockCall, locker: str, ttl: int = 0, nodes=None) -> LockResult:
        if nodes is None:
            nodes = self.nodes
        results = await self.request_from_nodes(
            f"{call}?locker={locker}&ttl={ttl}",
            method="post",
            ignore_status_code=True,
            json=False,
            caller="request_lock_op_from_nodes"
        )
        logger.debug("%s results: %r", call, results)
        if call in [LockCall.lock, LockCall.relock]:
            expected_result = {"locked": True}
        elif call in [LockCall.unlock]:
            expected_result = {"locked": False}
        else:
            raise NotImplementedError(f"Unknown lock call: {call!r}")
        rv = LockResult.ok
        for node, result in zip(nodes, results):
            if result is None or isinstance(result, Exception):
                logger.info("Exception occurred when talking with node %r: %r", node, result)
                if rv != LockResult.failure:
                    # failures mean that we're done, so don't override them
                    rv = LockResult.exception
            elif result.is_error:
                logger.info("%s of %s failed - unexpected result %r %r", call, node, result.status_code, result)
                rv = LockResult.failure
            else:
                try:
                    decoded_result = result.json()
                except json.JSONDecodeError:
                    decoded_result = None
                if decoded_result != expected_result:
                    logger.info("%s of %s failed - unexpected result %r", call, node, decoded_result)
                    rv = LockResult.failure
        return rv

    async def request_lock_from_nodes(self, *, locker: str, ttl: int) -> bool:
        return await self.request_lock_call_from_nodes(call=LockCall.lock, locker=locker, ttl=ttl) == LockResult.ok

    async def request_unlock_from_nodes(self, *, locker: str) -> bool:
        return await self.request_lock_call_from_nodes(call=LockCall.unlock, locker=locker) == LockResult.ok

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
        delay = self.config.poll_delay_start
        results = [None] * len(urls)
        # Note that we don't have timeout mechanism here as such,
        # however, if re-locking times out, we will bail out. TBD if
        # we need timeout mechanism here anyway.
        failures = {}
        async for _ in utils.exponential_backoff(
            initial=delay,
            multiplier=self.config.poll_delay_multiplier,
            maximum=self.config.poll_delay_max,
            duration=self.config.poll_duration
        ):
            for i, (url, result) in enumerate(zip(urls, results)):
                # TBD: This could be done in parallel too
                if result is not None and result.progress.final:
                    continue
                r = await utils.httpx_request(url, caller="BackupOp.wait_successful_results")
                if r is None:
                    failures[i] = failures.get(i, 0) + 1
                    if failures[i] >= self.config.poll_maximum_failures:
                        return []
                    continue
                # We got something -> decode the result
                result = result_class.parse_obj(r)
                results[i] = result
                if result.progress.finished_failed:
                    return []
            if not any(True for result in results if result is None or not result.progress.final):
                break
        else:
            logger.debug("wait_successful_results timed out")
            return []
        return results


class CoordinatorOpWithClusterLock(CoordinatorOp):
    def __init__(self, *, c: "Coordinator"):
        super().__init__(c=c)
        self.ttl = self.config.default_lock_ttl
        self.initial_lock_start = time.monotonic()
        self.locker = self.get_locker()

    def get_locker(self):
        return f"{socket.gethostname()}-{id(self)}"

    async def run(self):
        relock_tasks = []
        # Acquire initial locks
        try:
            r = await self.request_lock_from_nodes(locker=self.locker, ttl=self.ttl)
            if r:
                logger.debug("Locks acquired, creating relock tasks")
                relock_tasks = await self._create_relock_tasks()
                logger.debug("Calling run_with_lock")
                await self.run_with_lock()
            else:
                logger.info("Initial lock failed")
                self.set_status_fail()
        finally:
            if relock_tasks:
                for task in relock_tasks:
                    task.cancel()
                await asyncio.gather(*relock_tasks, return_exceptions=True)
            await self.request_unlock_from_nodes(locker=self.locker)

    async def _create_relock_tasks(self):
        current_task = asyncio.current_task()
        return [asyncio.create_task(self._node_relock_loop(current_task, node)) for node in self.nodes]

    async def _node_relock_loop(self, main_task, node):
        lock_eol = self.initial_lock_start + self.ttl
        next_lock = self.initial_lock_start + self.ttl / 2
        while True:
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
            r = await self.request_lock_call_from_nodes(
                call=magic.LockCall.relock, locker=self.locker, ttl=self.ttl, nodes=[node]
            )
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


class Coordinator(op.OpMixin):
    """ Convenience dependency which contains sub-dependencies most API endpoints need """
    def __init__(
        self,
        *,
        request: Request,
        background_tasks: BackgroundTasks,
        config: CoordinatorConfig = Depends(coordinator_config),
        state: CoordinatorState = Depends(coordinator_state)
    ):
        self.request = request
        self.background_tasks = background_tasks
        self.config = config
        self.state = state
