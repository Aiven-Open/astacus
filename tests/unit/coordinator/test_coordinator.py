# Copyright (c) 2024 Aiven Ltd
from astacus.common.ipc import Plugin
from astacus.common.statsd import StatsClient
from astacus.coordinator.cluster import Cluster, WaitResultError
from astacus.coordinator.config import CoordinatorConfig
from astacus.coordinator.coordinator import Coordinator, SteppedCoordinatorOp
from astacus.coordinator.plugins.base import Step, StepFailedError, StepsContext
from astacus.coordinator.state import CoordinatorState
from collections.abc import Callable
from fastapi import BackgroundTasks
from starlette.datastructures import URL
from typing import TypeAlias
from unittest.mock import Mock

import dataclasses
import pytest

ExceptionClosure: TypeAlias = Callable[[], Exception]


@dataclasses.dataclass
class FailingDummyStep(Step[None]):
    raised_exception: ExceptionClosure
    failure_handled: bool = dataclasses.field(init=False, default=False)

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        raise self.raised_exception()

    async def handle_step_failure(self, cluster: Cluster, context: StepsContext) -> None:
        self.failure_handled = True


@pytest.mark.parametrize("raised_exception", [lambda: StepFailedError, lambda: WaitResultError])
async def test_failure_handler_is_called(raised_exception: ExceptionClosure) -> None:
    coordinator = Coordinator(
        request_url=URL(),
        background_tasks=BackgroundTasks(),
        config=CoordinatorConfig(plugin=Plugin.files),
        state=CoordinatorState(),
        stats=StatsClient(config=None),
        storage_factory=Mock(),
    )
    dummy_step = FailingDummyStep(raised_exception=raised_exception)
    operation = SteppedCoordinatorOp(
        c=coordinator,
        attempts=1,
        steps=[dummy_step],
        operation_context=Mock(),
    )
    cluster = Cluster(nodes=[])
    context = StepsContext()
    result = await operation.try_run(cluster, context)
    assert dummy_step.failure_handled
    assert not result
