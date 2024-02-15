"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from astacus.common.threadlocal import CopiedThreadLocal
from typing import ClassVar, Self

import concurrent.futures


class State:
    num_instances: ClassVar[int] = 0

    def __init__(self) -> None:
        self.instance_no = State.num_instances
        State.num_instances += 1

    def copy(self) -> Self:
        return self.__class__()


def test_copied_thread_local() -> None:
    prototype = State()
    assert prototype.instance_no == 0
    state = CopiedThreadLocal(value=prototype)

    def test() -> int:
        return state.value.instance_no

    with concurrent.futures.ThreadPoolExecutor() as executor:
        res = executor.submit(test)
    assert res.result() == 1
    assert state.value.instance_no == 2
