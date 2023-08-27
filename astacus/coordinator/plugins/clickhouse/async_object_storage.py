"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from abc import ABC, abstractmethod
from pathlib import Path
from rohmu import BaseTransfer
from rohmu.errors import FileNotFoundFromStorageError
from starlette.concurrency import run_in_threadpool
from typing import Any, Iterator, Mapping, Self, Sequence

import contextlib
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
    def get_config(self) -> Mapping[str, Any]:
        ...

    @abstractmethod
    async def list_items(self) -> list[ObjectStorageItem]:
        ...

    @abstractmethod
    async def delete_item(self, key: Path) -> None:
        ...

    @abstractmethod
    async def copy_items_from(self, source: "AsyncObjectStorage", keys: Sequence[Path]) -> None:
        ...


class ThreadSafeRohmuStorage:
    def __init__(self, config: Mapping[str, Any]) -> None:
        self.config = config
        self._storage = rohmu.get_transfer(config)
        self._storage_lock = threading.Lock()

    def list_iter(self, key: str, *, with_metadata: bool = True, deep: bool = False) -> list[Mapping[str, Any]]:
        with self._storage_lock:
            return self._storage.list_iter(key, with_metadata=with_metadata, deep=deep)

    def delete_key(self, key: str) -> None:
        with self._storage_lock:
            self._storage.delete_key(key)

    def copy_items_from(self, source: "ThreadSafeRohmuStorage", keys: Sequence[str]) -> None:
        # In theory this could deadlock if some other place was locking the same two storages
        # in the reverse order at the same time. Within the context of backups and restore,
        # it's quite unlikely to have a pair of storages be the source and target of each other.
        # Especially since we create new storage objects for each coordinator operation.
        with source.get_storage() as source_storage:
            with self.get_storage() as target_storage:
                target_storage.copy_files_from(source=source_storage, keys=keys)

    @contextlib.contextmanager
    def get_storage(self) -> Iterator[BaseTransfer]:
        with self._storage_lock:
            yield self._storage


@dataclasses.dataclass(frozen=True)
class RohmuAsyncObjectStorage(AsyncObjectStorage):
    storage: ThreadSafeRohmuStorage

    def get_config(self) -> Mapping[str, Any]:
        return self.storage.config

    async def list_items(self) -> list[ObjectStorageItem]:
        items = await run_in_threadpool(self.storage.list_iter, key="", with_metadata=True, deep=True)
        return [ObjectStorageItem(key=Path(item["name"]), last_modified=item["last_modified"]) for item in items]

    async def delete_item(self, key: Path) -> None:
        await run_in_threadpool(self.storage.delete_key, str(key))

    async def copy_items_from(self, source: "AsyncObjectStorage", keys: Sequence[Path]) -> None:
        if not isinstance(source, RohmuAsyncObjectStorage):
            raise NotImplementedError("Copying items is only supported from another RohmuAsyncObjectStorage")
        str_keys = [str(key) for key in keys]
        await run_in_threadpool(self.storage.copy_items_from, source.storage, str_keys)


@dataclasses.dataclass(frozen=True)
class MemoryAsyncObjectStorage(AsyncObjectStorage):
    items: dict[Path, ObjectStorageItem] = dataclasses.field(default_factory=dict)

    @classmethod
    def from_items(cls, items: Sequence[ObjectStorageItem]) -> Self:
        return cls(items={item.key: item for item in items})

    def get_config(self) -> Mapping[str, Any]:
        # Exposing the object id in the config ensures that the same memory storage
        # has the same config as itself and a different config as another memory storage.
        # Using a manually picked name would be more error-prone: we want two object
        # storages to share state when their config is equal (= they are reading and
        # writing to the same place), that wouldn't happen with two identically named
        # *memory* object storages.
        return {"memory_id": id(self)}

    async def list_items(self) -> list[ObjectStorageItem]:
        return list(self.items.values())

    async def delete_item(self, key: Path) -> None:
        if key not in self.items:
            raise FileNotFoundFromStorageError(key)
        logger.info("deleting item: %r", key)
        self.items.pop(key)

    async def copy_items_from(self, source: "AsyncObjectStorage", keys: Sequence[Path]) -> None:
        keys_set = set(keys)
        for source_item in await source.list_items():
            if source_item.key in keys_set:
                self.items[source_item.key] = source_item
