"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

astacus.common.op tests that do not fit elsewhere

"""

from astacus.common import op
from unittest.mock import MagicMock

import inspect
import pytest


class MockBackgroundTasks(list):
    def add_task(self, task):
        self.append(task)

    async def run(self):
        while self:
            fun = self.pop()
            if inspect.iscoroutinefunction(fun):
                await fun()
            else:
                fun()


class MockOp:
    status = None

    def set_status(self, state, *, from_state=None):
        if from_state and from_state != self.status:
            return
        self.status = state

    def set_status_fail(self):
        self.status = op.Op.Status.fail


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fun_ex,expect_status,expect_ex",
    [
        # Non-failing operation winds up in None
        (None, op.Op.Status.done, None),
        # If operation throws ExpiredOperationException, op status
        # should stay running as it may point to the next operation
        (op.ExpiredOperationException, op.Op.Status.running, None),
        # If operation throws 'something else', it should fail the op status
        (AssertionError, op.Op.Status.fail, AssertionError),
    ]
)
@pytest.mark.parametrize("is_async", [False, True])
async def test_opmixin_start_op(is_async, fun_ex, expect_status, expect_ex):
    bg_tasks = MockBackgroundTasks()
    mixin = op.OpMixin()
    mixin.state = MagicMock()
    mixin.stats = MagicMock()
    mixin.request = MagicMock()
    mixin.request.url.scheme = "http"
    mixin.request.url.netloc = "localhost"
    mixin.request.url.path = ""
    mixin.background_tasks = bg_tasks
    op_obj = MockOp()

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
        await bg_tasks.run()
    except Exception as ex:  # pylint: disable=broad-except
        assert expect_ex and isinstance(ex, expect_ex)
    assert op_obj.status == expect_status
