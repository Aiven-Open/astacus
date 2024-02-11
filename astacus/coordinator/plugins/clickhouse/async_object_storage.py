"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from abc import ABC, abstractmethod
from astacus.common.rohmustorage import RohmuStorageConfig
from collections.abc import Iterator, Mapping, Sequence
from rohmu import BaseTransfer
from rohmu.errors import FileNotFoundFromStorageError
from starlette.concurrency import run_in_threadpool
from typing import Any, Self, Union

import contextlib
import dataclasses
import datetime
import logging
import rohmu
import threading

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class ObjectStorageItem:
    key: str
    last_modified: datetime.datetime


class AsyncObjectStorage(ABC):
    @abstractmethod
    def get_config(self) -> Union[RohmuStorageConfig, dict]:
        ...

    @abstractmethod
    async def list_items(self) -> list[ObjectStorageItem]:
        ...

    @abstractmethod
    async def delete_item(self, key: str) -> None:
        ...

    @abstractmethod
    async def copy_items_from(self, source: "AsyncObjectStorage", keys: Sequence[str]) -> None:
        ...


class ThreadSafeRohmuStorage:
    def __init__(self, config: RohmuStorageConfig) -> None:
        self.config = config
        self._storage = rohmu.get_transfer_from_model(config)
        self._storage_lock = threading.Lock()

    def list_iter(self, key: str, *, with_metadata: bool = True, deep: bool = False) -> Iterator[Mapping[str, Any]]:
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
    def get_storage(self) -> Iterator[BaseTransfer[Any]]:
        with self._storage_lock:
            yield self._storage


@dataclasses.dataclass(frozen=True)
class RohmuAsyncObjectStorage(AsyncObjectStorage):
    storage: ThreadSafeRohmuStorage

    def get_config(self) -> RohmuStorageConfig:
        return self.storage.config

    async def list_items(self) -> list[ObjectStorageItem]:
        items = await run_in_threadpool(self.storage.list_iter, key="", with_metadata=True, deep=True)
        return [ObjectStorageItem(key=item["name"], last_modified=item["last_modified"]) for item in items]

    async def delete_item(self, key: str) -> None:
        await run_in_threadpool(self.storage.delete_key, key)

    async def copy_items_from(self, source: "AsyncObjectStorage", keys: Sequence[str]) -> None:
        if not isinstance(source, RohmuAsyncObjectStorage):
            raise NotImplementedError("Copying items is only supported from another RohmuAsyncObjectStorage")
        await run_in_threadpool(self.storage.copy_items_from, source.storage, keys)


@dataclasses.dataclass(frozen=True)
class MemoryAsyncObjectStorage(AsyncObjectStorage):
    items: dict[str, ObjectStorageItem] = dataclasses.field(default_factory=dict)

    @classmethod
    def from_items(cls, items: Sequence[ObjectStorageItem]) -> Self:
        return cls(items={item.key: item for item in items})

    def get_config(self) -> dict:
        # Exposing the object id in the config ensures that the same memory storage
        # has the same config as itself and a different config as another memory storage.
        # Using a manually picked name would be more error-prone: we want two object
        # storages to share state when their config is equal (= they are reading and
        # writing to the same place), that wouldn't happen with two identically named
        # *memory* object storages.
        return {"memory_id": id(self)}

    async def list_items(self) -> list[ObjectStorageItem]:
        return list(self.items.values())

    async def delete_item(self, key: str) -> None:
        if key not in self.items:
            raise FileNotFoundFromStorageError(key)
        logger.info("deleting item: %r", key)
        self.items.pop(key)

    async def copy_items_from(self, source: "AsyncObjectStorage", keys: Sequence[str]) -> None:
        keys_set = set(keys)
        for source_item in await source.list_items():
            if source_item.key in keys_set:
                self.items[source_item.key] = source_item
