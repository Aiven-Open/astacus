"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from abc import ABC, abstractmethod
from pathlib import Path
from rohmu.errors import FileNotFoundFromStorageError
from starlette.concurrency import run_in_threadpool
from typing import Any, Mapping

import dataclasses
import datetime
import logging
import rohmu
import threading

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class ObjectStorageItem:
    key: Path
    last_modified: datetime.datetime


class AsyncObjectStorage(ABC):
    @abstractmethod
    async def list_items(self) -> list[ObjectStorageItem]:
        ...

    @abstractmethod
    async def delete_item(self, key: Path) -> None:
        ...


class ThreadSafeRohmuStorage:
    def __init__(self, config: Mapping[str, Any]) -> None:
        self._storage = rohmu.get_transfer(config)
        self._storage_lock = threading.Lock()

    def list_iter(self, key: str, *, with_metadata: bool = True, deep: bool = False) -> list[Mapping[str, Any]]:
        with self._storage_lock:
            return self._storage.list_iter(key, with_metadata=with_metadata, deep=deep)

    def delete_key(self, key: str) -> None:
        with self._storage_lock:
            self._storage.delete_key(key)


@dataclasses.dataclass(frozen=True)
class RohmuAsyncObjectStorage(AsyncObjectStorage):
    storage: ThreadSafeRohmuStorage

    async def list_items(self) -> list[ObjectStorageItem]:
        items = await run_in_threadpool(self.storage.list_iter, key="", with_metadata=True, deep=True)
        return [ObjectStorageItem(key=Path(item["name"]), last_modified=item["last_modified"]) for item in items]

    async def delete_item(self, key: Path) -> None:
        await run_in_threadpool(self.storage.delete_key, str(key))


@dataclasses.dataclass(frozen=True)
class MemoryAsyncObjectStorage(AsyncObjectStorage):
    items: list[ObjectStorageItem]

    async def list_items(self) -> list[ObjectStorageItem]:
        return self.items[:]

    async def delete_item(self, key: Path) -> None:
        for item in self.items:
            if item.key == key:
                logger.info("deleting item: %r", key)
                self.items.remove(item)
                return
        raise FileNotFoundFromStorageError(key)
