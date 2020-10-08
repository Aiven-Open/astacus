"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

"""

from .utils import AstacusModel

import logging
import math

logger = logging.getLogger(__name__)

_log_1_1 = math.log(1.1)


def increase_worth_reporting(value, new_value=None, *, total=None):
    """ Make reporting sparser and sparser as values grow larger
    - report every 1.1**N or so
    - if we know total, report every percent
    """
    if new_value is None:
        new_value = value
        value = new_value - 1
    if total is not None:
        if new_value == total or total <= 100:
            return True
        old_percent = 100 * value // total
        new_percent = 100 * new_value // total
        return old_percent != new_percent
    if value <= 10 or new_value <= 10:
        return True
    old_exp = int(math.log(value) / _log_1_1)
    new_exp = int(math.log(new_value) / _log_1_1)
    return old_exp != new_exp


class Progress(AstacusModel):
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
        old_total = self.total
        self.total += n
        if increase_worth_reporting(old_total, self.total):
            logger.debug("add_total %r -> %r", n, self)
        assert not self.final

    def add_fail(self, n=1, *, info="add_fail"):
        assert n > 0
        old_failed = self.failed
        self.failed += n
        if increase_worth_reporting(old_failed, self.failed):
            logger.debug("%s %r -> %r", info, n, self)
        assert not self.final

    def add_success(self, n=1, *, info="add_success"):
        assert n > 0
        old_handled = self.handled
        self.handled += n
        assert self.handled <= self.total
        if increase_worth_reporting(old_handled, self.handled, total=self.total):
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

    @property
    def finished_failed(self):
        return self.final and not self.finished_successfully
