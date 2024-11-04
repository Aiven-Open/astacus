"""Copyright (c) 2021 Aiven Ltd
See LICENSE for details.
"""

from astacus.common import ipc, op, utils
from astacus.common.magic import LockCall
from astacus.common.progress import Progress
from astacus.common.statsd import StatsClient
from astacus.common.utils import AsyncSleeper, httpx_request_stream
from astacus.coordinator.config import CoordinatorNode, PollConfig
from collections.abc import Callable, Mapping, Sequence
from enum import Enum
from typing import Any, cast, TypeAlias, TypeVar

import asyncio
import copy
import httpx
import json
import logging
import msgspec
import urllib.parse

logger = logging.getLogger(__name__)

T = TypeVar("T")
NR = TypeVar("NR", bound=ipc.NodeResult)
Result: TypeAlias = BaseException | httpx.Response | Mapping[str, Any]


class LockResult(Enum):
    ok = "ok"
    failure = "failure"
    exception = "exception"


class Cluster:
    def __init__(
        self,
        *,
        nodes: Sequence[CoordinatorNode],
        poll_config: PollConfig | None = None,
        subresult_url: str | None = None,
        subresult_sleeper: AsyncSleeper | None = None,
        stats: StatsClient | None = None,
    ):
        self.nodes = nodes
        self.poll_config = PollConfig() if poll_config is None else poll_config
        self.subresult_url = subresult_url
        self.subresult_sleeper = subresult_sleeper
        self.stats = stats
        self.progress_handler: Callable[[Progress], None] | None = None

    def set_progress_handler(self, progress_handler: Callable[[Progress], None] | None):
        self.progress_handler = progress_handler

    async def request_lock(self, *, locker: str, ttl: int) -> LockResult:
        return await self._request_lock_call_from_nodes(call=LockCall.lock, locker=locker, ttl=ttl, nodes=self.nodes)

    async def request_unlock(self, *, locker: str) -> LockResult:
        return await self._request_lock_call_from_nodes(call=LockCall.unlock, locker=locker, nodes=self.nodes)

    async def request_relock(self, *, node: CoordinatorNode, locker: str, ttl: int) -> LockResult:
        assert node in self.nodes
        return await self._request_lock_call_from_nodes(call=LockCall.relock, locker=locker, ttl=ttl, nodes=[node])

    async def request_from_nodes(
        self,
        url: str,
        *,
        caller: str,
        req: ipc.NodeRequest | None = None,
        reqs: Sequence[ipc.NodeRequest] | None = None,
        nodes: Sequence[CoordinatorNode] | None = None,
        **kw,
    ) -> Sequence[Result | None]:
        """Perform asynchronously parallel request to the node components.

        The 'url' specifies sub-url under the node's own url. The set
        of nodes is by default all nodes, unless specified as
        argument.

        The request to be given can be provided either using 'req' (in
        which case all nodes get the same request), or 'reqs' iterable
        which must provide one entry for each covered node. It can be
        also omitted in which case empty ipc.NodeRequest is used.
        """
        if nodes is None:
            nodes = self.nodes
        else:
            nodes = list(nodes)

        if not nodes:
            return []

        if reqs is not None:
            assert req is None
            reqs = list(reqs)
        else:
            if req is None:
                req = ipc.NodeRequest()
            reqs = [copy.deepcopy(req) for _ in nodes]
        assert reqs

        if self.subresult_url:
            for subreq in reqs:
                assert isinstance(subreq, ipc.NodeRequest)
                subreq.result_url = self.subresult_url

        urls = [f"{node.url}/{url}" for node in nodes]
        assert len(reqs) == len(urls)

        # Now 'reqs' + 'urls' contains all we need to actually perform
        # requests we want to.
        aws = [
            utils.httpx_request(url, caller=caller, content=msgspec.json.encode(req), **kw) for req, url in zip(reqs, urls)
        ]
        results = await asyncio.gather(*aws, return_exceptions=True)

        logger.info("request_from_nodes %r to %r => %r", reqs, urls, results)
        return results

    async def _request_lock_call_from_nodes(
        self, *, call: LockCall, locker: str, ttl: int = 0, nodes: Sequence[CoordinatorNode]
    ) -> LockResult:
        results = await self.request_from_nodes(
            f"{call}?locker={locker}&ttl={ttl}",
            method="post",
            ignore_status_code=True,
            json=False,
            nodes=nodes,
            caller="Cluster._request_lock_call_from_nodes",
        )
        logger.info("%s results: %r", call, results)
        if call in [LockCall.lock, LockCall.relock]:
            expected_result = {"locked": True}
        elif call in [LockCall.unlock]:
            expected_result = {"locked": False}
        else:
            raise NotImplementedError(f"Unknown lock call: {call!r}")
        rv = LockResult.ok
        for node, result in zip(nodes, results):
            # This assert helps mypy handle request_from_nodes return type dependent on its json parameter
            assert not isinstance(result, Mapping)
            if result is None or isinstance(result, BaseException):
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
        if rv == LockResult.failure and self.stats is not None:
            self.stats.increase(
                "astacus_lock_call_failure",
                tags={
                    "call": call,
                    "locker": locker,
                },
            )
        return rv

    async def wait_successful_results(
        self, *, start_results: Sequence[Result | None], result_class: type[NR]
    ) -> Sequence[NR]:
        urls = []

        for i, start_result in enumerate(start_results, 1):
            if not start_result or isinstance(start_result, BaseException):
                logger.info(
                    "wait_successful_results: Incorrect start result for #%d/%d: %r", i, len(start_results), start_result
                )
                raise WaitResultError(f"incorrect start result for #{i}/{len(start_results)}: {start_result!r}")
            parsed_start_result = op.Op.StartResult.parse_obj(start_result)
            urls.append(parsed_start_result.status_url)
        if len(urls) != len(start_results):
            raise WaitResultError(f"incorrect number of results: {len(urls)} vs {len(start_results)}")
        results: list[NR | None] = [None] * len(urls)
        # Note that we don't have timeout mechanism here as such,
        # however, if re-locking times out, we will bail out. TBD if
        # we need timeout mechanism here anyway.
        failures = {i: 0 for i in range(len(results))}

        async for _ in utils.exponential_backoff(
            initial=self.poll_config.delay_start,
            multiplier=self.poll_config.delay_multiplier,
            maximum=self.poll_config.delay_max,
            duration=self.poll_config.duration,
            async_sleeper=self.subresult_sleeper,
        ):
            for i, (url, result) in enumerate(zip(urls, results)):
                # TBD: This could be done in parallel too
                if result is not None and result.progress.final:
                    continue
                progress_text = f"{result.progress!r}" if result is not None else "not started"
                logger.info("%s node #%d/%d: %s", node_op_from_url(url), i, len(urls), progress_text)
                async with httpx_request_stream(
                    url, caller="Nodes.wait_successful_results", timeout=self.poll_config.result_timeout
                ) as r:
                    if r is None:
                        failures[i] += 1
                        if failures[i] >= self.poll_config.maximum_failures:
                            raise WaitResultError("too many failures")
                        continue
                    # We got something -> decode the result
                    assert isinstance(r, httpx.Response)
                    payload = bytearray()
                    async for chunk in r.aiter_bytes():
                        payload.extend(chunk)
                result = msgspec.json.decode(payload, type=result_class)
                results[i] = result
                failures[i] = 0
                if self.progress_handler is not None:
                    self.progress_handler(Progress.merge(r.progress for r in results if r is not None))
                if result.progress.finished_failed:
                    raise WaitResultError
            if not any(True for result in results if result is None or not result.progress.final):
                break
        else:
            logger.info("wait_successful_results timed out")
            raise WaitResultError("timed out")
        # The case is valid because we get there when all results are not None
        return cast(Sequence[NR], results)


class WaitResultError(Exception):
    pass


def node_op_from_url(url: str) -> str:
    parsed_url = urllib.parse.urlparse(url)
    return parsed_url.path.replace("/node/", "")
