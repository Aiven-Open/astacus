"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

"""

from pydantic import BaseModel  # pylint: disable=no-name-in-module # ( sometimes Cython -> pylint won't work )

import logging

logger = logging.getLogger(__name__)


class Progress(BaseModel):
    """ JSON-encodable progress meter of sorts """
    handled: int = 0
    failed: int = 0
    total: int = 0
    final: bool = False

    def __repr__(self):
        finished = ", finished" if self.final else ""
        return f"{self.handled}/{self.total} handled, {self.failed} failures{finished}"

    def start(self, n):
        " Optional 'first' step, just for logic handling state (e.g. no progress object reuse desired) "
        assert not self.total
        logger.debug("start")
        self.add_total(n)

    def add_total(self, n):
        if not n:
            return
        self.total += n
        logger.debug("add_total %r -> %r", n, self)
        assert not self.final

    def add_fail(self, n=1, *, info="add_fail"):
        assert n > 0
        self.failed += n
        logger.debug("%s %r -> %r", info, n, self)
        assert not self.final

    def add_success(self, n=1, *, info="add_success"):
        assert n > 0
        self.handled += n
        assert self.handled <= self.total
        logger.debug("%s %r -> %r", info, n, self)
        assert not self.final

    def download_success(self, size):
        self.add_success(size, info="download_success")

    def upload_success(self, hexdigest):
        self.add_success(info=f"upload_success {hexdigest}")

    def upload_missing(self, hexdigest):
        self.add_fail(info=f"upload_missing {hexdigest}")

    def upload_failure(self, hexdigest):
        self.add_fail(info=f"upload_failure {hexdigest}")

    def done(self):
        assert self.total is not None and self.handled <= self.total
        assert not self.final
        self.final = True
        logger.debug("done %r", self)

    @property
    def finished_successfully(self):
        return self.final and not self.failed and self.handled == self.total
