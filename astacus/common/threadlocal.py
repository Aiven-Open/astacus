"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from typing import cast, Generic, Protocol, Self, TypeVar

import threading


class Copyable(Protocol):
    def copy(self) -> Self:
        return self


T = TypeVar("T", bound=Copyable)


class CopiedThreadLocal(Generic[T]):
    """Wrap a value and copy it to a thread-local storage on first
    access for each thread.
    """

    def __init__(self, *, value: T) -> None:
        self._threadlocal = threading.local()
        self._prototype: T = value

    @property
    def value(self) -> T:
        local_storage = getattr(self._threadlocal, "value", None)
        if local_storage is None:
            local_storage = self._prototype.copy()
            setattr(self._threadlocal, "value", local_storage)
        return cast(T, local_storage)
