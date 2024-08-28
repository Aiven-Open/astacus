"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""


from astacus.common.statsd import StatsClient
from astacus.coordinator.plugins.clickhouse.object_storage import ObjectStorage, ObjectStorageItem
from collections.abc import Sequence
from rohmu.errors import FileNotFoundFromStorageError
from typing import Self

import dataclasses
import logging

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class MemoryObjectStorage(ObjectStorage):
    items: dict[str, ObjectStorageItem] = dataclasses.field(default_factory=dict)

    def close(self) -> None:
        pass

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

    def list_items(self) -> list[ObjectStorageItem]:
        return list(self.items.values())

    def delete_item(self, key: str) -> None:
        if key not in self.items:
            raise FileNotFoundFromStorageError(key)
        logger.info("deleting item: %r", key)
        self.items.pop(key)

    def copy_items_from(self, source: "ObjectStorage", keys: Sequence[str], *, stats: StatsClient | None) -> None:
        keys_set = set(keys)
        for source_item in source.list_items():
            if source_item.key in keys_set:
                self.items[source_item.key] = source_item
