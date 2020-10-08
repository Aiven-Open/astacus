"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

astacus.common.progress tests

"""

from astacus.common import progress

import pytest


@pytest.mark.parametrize(
    "old_value,new_value,total,exp",
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
    ]
)
def test_increase_worth_reporting(old_value, new_value, total, exp):
    assert progress.increase_worth_reporting(old_value, new_value, total=total) == exp
