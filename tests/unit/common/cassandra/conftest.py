"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from tests.utils import is_cassandra_driver_importable

collect_ignore_glob = [] if is_cassandra_driver_importable() else ["*.py"]
