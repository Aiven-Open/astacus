"""Copyright (c) 2020 Aiven Ltd
See LICENSE for details.

astacus.common.progress tests

"""

from astacus.common import progress

import pytest


@pytest.mark.parametrize(
    ("old_value", "new_value", "total", "exp"),
    [
        (0, 1, None, True),
        (8, 9, None, True),
        (8, 9, 10, True),
        # Test 1.1^N logic
        (16674, 16675, None, True),  # 16674.5
        (16675, 18340, None, False),  # 18341.x
        # Test every percent logic
        (999, 1000, 100_000, True),
        (1000, 1999, 100_000, False),
        (1999, 2000, 100_000, True),
        # Test +1 logic
        (1, None, None, True),
        (16674, None, None, False),
        (16675, None, None, True),
        (16676, None, None, False),
    ],
)
def test_increase_worth_reporting(old_value: int, new_value: int | None, total: int | None, exp: bool) -> None:
    assert progress.increase_worth_reporting(old_value, new_value, total=total) == exp


@pytest.mark.parametrize("is_final", [True, False])
def test_progress_merge(is_final: bool) -> None:
    p1 = progress.Progress(handled=0, failed=1000, total=10, final=True)
    p2 = progress.Progress(handled=100, failed=100, total=1000, final=is_final)
    p3 = progress.Progress(handled=1000, failed=10, total=10000, final=True)
    p4 = progress.Progress(handled=10000, failed=1, total=100000, final=True)

    p = progress.Progress.merge([p1, p2, p3, p4])
    assert p.handled == 11100
    assert p.failed == 1111
    assert p.total == 111010
    assert p.final == is_final
