"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

"""

from collections.abc import Iterable, Iterator, Sequence
from typing import Self, TypeVar

import logging
import math
import msgspec

logger = logging.getLogger(__name__)

_log_1_1 = math.log(1.1)


def increase_worth_reporting(value: int, new_value: int | None = None, *, total: int | None = None):
    """Make reporting sparser and sparser as values grow larger
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


T = TypeVar("T")


class Progress(msgspec.Struct, kw_only=True):
    """JSON-encodable progress meter of sorts"""

    handled: int = 0
    failed: int = 0
    total: int = 0
    final: bool = False

    def __repr__(self) -> str:
        finished = ", finished" if self.final else ""
        return f"{self.handled}/{self.total} handled, {self.failed} failures{finished}"

    def wrap(self, i: Sequence[T]) -> Iterator[T]:
        """Iterate over i, updating progress as we go."""
        try:
            self.add_total(len(i))
        except TypeError:
            # Can't compute progress for this iterator
            yield from i
            return None

        for item in i:
            yield item
            self.add_success()
        return None

    def start(self, n) -> None:
        "Optional 'first' step, just for logic handling state (e.g. no progress object reuse desired)"
        assert not self.total
        logger.info("start")
        self.add_total(n)

    def add_total(self, n: int) -> None:
        if not n:
            return
        old_total = self.total
        self.total += n
        if increase_worth_reporting(old_total, self.total):
            logger.info("add_total %r -> %r", n, self)
        assert not self.final

    def add_fail(self, n: int = 1, *, info: str = "add_fail") -> None:
        assert n > 0
        old_failed = self.failed
        self.failed += n
        if increase_worth_reporting(old_failed, self.failed):
            logger.info("%s %r -> %r", info, n, self)
        assert not self.final

    def add_success(self, n: int = 1, *, info: str = "add_success") -> None:
        assert n > 0
        old_handled = self.handled
        self.handled += n
        assert self.handled <= self.total
        if increase_worth_reporting(old_handled, self.handled, total=self.total):
            logger.info("%s %r -> %r", info, n, self)
        assert not self.final

    def download_success(self, size: int) -> None:
        self.add_success(size, info="download_success")

    def upload_success(self, hexdigest: str) -> None:
        self.add_success(info=f"upload_success {hexdigest}")

    def upload_missing(self, hexdigest: str) -> None:
        self.add_fail(info=f"upload_missing {hexdigest}")

    def upload_failure(self, hexdigest: str) -> None:
        self.add_fail(info=f"upload_failure {hexdigest}")

    def done(self) -> None:
        assert self.total is not None and self.handled <= self.total
        assert not self.final
        self.final = True
        logger.info("done %r", self)

    @property
    def finished_successfully(self) -> bool:
        return self.final and not self.failed and self.handled == self.total

    @property
    def finished_failed(self) -> bool:
        return self.final and not self.finished_successfully

    @classmethod
    def merge(cls, progresses: Iterable[Self]) -> Self:
        p = cls()
        for progress in progresses:
            p.handled += progress.handled
            p.failed += progress.failed
            p.total += progress.total
        p.final = all(progress.final for progress in progresses)
        return p
