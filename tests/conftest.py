"""
Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""

import platform
import pytest


def pytest_runtest_setup(item):
    if any(item.iter_markers(name="x86_64")) and platform.machine() != "x86_64":

        pytest.skip("x86_64 arch required")
