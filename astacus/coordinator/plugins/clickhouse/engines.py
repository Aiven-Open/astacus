"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

import enum


# This enum contains only used constants
class TableEngine(enum.Enum):
    MySQL = "MySQL"
    PostgreSQL = "PostgreSQL"
    S3 = "S3"
