"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

"""

import logging

logger = logging.getLogger(__name__)


class DebugProgress:
    def __init__(self):
        self.i = 0
        self.n = None

    def start(self, n):
        self.n = n
        logger.debug("start %r", self.n)

    def download_success(self, size):
        self.i += size
        logger.debug("download_success %r -> %r/%r", size, self.i, self.n)

    def upload_success(self, hexdigest):
        logger.debug("upload_success %r", hexdigest)
        self.i += 1

    def upload_failure(self, hexdigest):
        logger.debug("upload_failure %r", hexdigest)

    def done(self):
        assert self.n is not None and self.i <= self.n
        logger.debug("done %r/%r", self.i, self.n)
