"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""


from astacus.coordinator.plugins.base import StepsContext
from tests.utils import is_cassandra_driver_importable

import pytest

collect_ignore_glob = [] if is_cassandra_driver_importable() else ["*.py"]


@pytest.fixture(name="context")
def fixture_context() -> StepsContext:
    return StepsContext()
