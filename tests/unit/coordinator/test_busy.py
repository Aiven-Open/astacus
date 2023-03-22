# Copyright (c) 2023 Aiven, Helsinki, Finland. https://aiven.io/


from astacus.common.op import Op
from astacus.common.statsd import StatsClient
from fastapi import FastAPI
from starlette.testclient import TestClient
from tests.unit.common.test_op_stats import DummyOp

import pytest


def test_not_busy_if_no_coordinator_state(app: FastAPI, client: TestClient) -> None:
    app.state.coordinator_state = None
    assert not client.get("/busy").json()


@pytest.mark.parametrize("finished_status", [Op.Status.fail, Op.Status.done])
def test_not_busy_if_failed_or_done(app: FastAPI, client: TestClient, finished_status: Op.Status) -> None:
    stats = StatsClient(config=None)
    operation = DummyOp(info=Op.Info(op_id=1, op_name="DummyOp", op_status=finished_status), op_id=1, stats=stats)
    app.state.coordinator_state.op = operation
    app.state.coordinator_state.op_info = operation.info
    assert not client.get("/busy").json()


@pytest.mark.parametrize("finished_status", [Op.Status.running, Op.Status.starting])
def test_busy_if_starting_or_running(app: FastAPI, client: TestClient, finished_status: Op.Status) -> None:
    stats = StatsClient(config=None)
    operation = DummyOp(info=Op.Info(op_id=1, op_name="DummyOp", op_status=finished_status), op_id=1, stats=stats)
    app.state.coordinator_state.op = operation
    app.state.coordinator_state.op_info = operation.info
    assert client.get("/busy").json()
