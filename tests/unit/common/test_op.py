"""Copyright (c) 2020 Aiven Ltd
See LICENSE for details.

astacus.common.op tests that do not fit elsewhere

"""

from astacus.common import op
from astacus.common.exceptions import ExpiredOperationException
from astacus.common.statsd import StatsClient
from starlette.background import BackgroundTasks
from starlette.datastructures import URL

import pytest


@pytest.mark.parametrize(
    ("fun_ex", "expect_status", "expect_ex"),
    [
        # Non-failing operation winds up in None
        (None, op.Op.Status.done, None),
        # If operation throws ExpiredOperationException, op status
        # should stay running as it may point to the next operation
        (ExpiredOperationException, op.Op.Status.running, None),
        # If operation throws 'something else', it should fail the op status
        (AssertionError, op.Op.Status.fail, AssertionError),
    ],
)
@pytest.mark.parametrize("is_async", [False, True])
async def test_opmixin_start_op(
    is_async: bool, fun_ex: type[Exception] | None, expect_status: op.Op.Status, expect_ex: type[Exception] | None
) -> None:
    mixin = op.OpMixin()
    mixin.state = op.OpState()
    mixin.stats = StatsClient(config=None)
    mixin.request_url = URL()
    mixin.background_tasks = BackgroundTasks()
    op_obj = op.Op(info=op.Op.Info(op_id=1), op_id=1, stats=StatsClient(config=None))

    def _sync():
        if fun_ex:
            raise fun_ex()

    async def _async():
        if fun_ex:
            raise fun_ex()

    try:
        if is_async:
            mixin.start_op(op=op_obj, op_name="dummy", fun=_async)
        else:
            mixin.start_op(op=op_obj, op_name="dummy", fun=_sync)
        await mixin.background_tasks()
    except Exception as ex:  # pylint: disable=broad-except
        assert expect_ex
        assert isinstance(ex, expect_ex)
    assert op_obj.info.op_status == expect_status
