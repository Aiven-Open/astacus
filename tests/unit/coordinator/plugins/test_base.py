"""

Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from astacus.coordinator.cluster import Cluster
from astacus.coordinator.plugins.base import Step, StepsContext

import datetime
import pytest


class DummyStep(Step[int]):
    async def run_step(self, cluster: Cluster, context: StepsContext) -> int:
        return 1


def test_steps_context_backup_name_is_prefixed_timestamp():
    context = StepsContext(attempt_start=datetime.datetime(2020, 1, 2, 3, 4, 5, 678912, tzinfo=datetime.timezone.utc))
    assert context.backup_name == "backup-2020-01-02T03:04:05+00:00"


def test_steps_context_result_can_be_set_and_retrieved():
    context = StepsContext()
    context.set_result(DummyStep, 10)
    assert context.get_result(DummyStep) == 10


def test_steps_context_missing_result_fails():
    context = StepsContext()
    with pytest.raises(LookupError):
        context.get_result(DummyStep)


def test_steps_context_result_can_only_be_set_once():
    context = StepsContext()
    context.set_result(DummyStep, 10)
    with pytest.raises(RuntimeError):
        context.set_result(DummyStep, 10)
