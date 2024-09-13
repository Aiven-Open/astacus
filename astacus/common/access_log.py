"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from collections.abc import Sequence

import logging


class AccessLogLevelFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        # Lower the log level of GET requests from INFO to DEBUG
        if isinstance(record.args, Sequence) and len(record.args) >= 2 and record.args[1] == "GET":
            record.levelno = logging.DEBUG
            record.levelname = logging.getLevelName(record.levelno)
        return True
