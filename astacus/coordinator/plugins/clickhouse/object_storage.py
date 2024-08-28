"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from .config import DirectCopyConfig, LocalCopyConfig
from abc import ABC, abstractmethod
from astacus.common.rohmustorage import RohmuStorageConfig
from astacus.common.statsd import StatsClient
from collections.abc import Iterator, Sequence
from rohmu import BaseTransfer
from typing import Any

import contextlib
import dataclasses
import datetime
import functools
import logging
import rohmu
import tempfile
import threading

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class ObjectStorageItem:
    key: str
    last_modified: datetime.datetime


class ObjectStorage(ABC):
    @abstractmethod
    def close(self) -> None:
        ...

    @abstractmethod
    def get_config(self) -> RohmuStorageConfig | dict:
        ...

    @abstractmethod
    def list_items(self) -> list[ObjectStorageItem]:
        ...

    @abstractmethod
    def delete_item(self, key: str) -> None:
        ...

    @abstractmethod
    def copy_items_from(self, source: "ObjectStorage", keys: Sequence[str], *, stats: StatsClient | None) -> None:
        ...


class ThreadSafeRohmuStorage(ObjectStorage):
    def __init__(self, *, config: RohmuStorageConfig, copy_config: DirectCopyConfig | LocalCopyConfig) -> None:
        self.config = config
        self.copy_config = copy_config
        self._storage = rohmu.get_transfer_from_model(config)
        self._storage_lock = threading.Lock()

    def close(self) -> None:
        self._storage.close()

    def get_config(self) -> RohmuStorageConfig | dict:
        return self.config

    def list_items(self) -> list[ObjectStorageItem]:
        with self._storage_lock:
            items = self._storage.list_iter(key="", with_metadata=True, deep=True)
        return [ObjectStorageItem(key=item["name"], last_modified=item["last_modified"]) for item in items]

    def delete_item(self, key: str) -> None:
        with self._storage_lock:
            self._storage.delete_key(key)

    def copy_items_from(self, source: ObjectStorage, keys: Sequence[str], *, stats: StatsClient | None) -> None:
        # In theory this could deadlock if some other place was locking the same two storages
        # in the reverse order at the same time. Within the context of backups and restore,
        # it's quite unlikely to have a pair of storages be the source and target of each other.
        # Especially since we create new storage objects for each coordinator operation.
        if not isinstance(source, ThreadSafeRohmuStorage):
            raise NotImplementedError("Copying items is only supported from another ThreadSafeRohmuStorage")
        with source.get_storage() as source_storage:
            with self.get_storage() as target_storage:
                copy_items_between(
                    keys,
                    copy_config=self.copy_config,
                    source_storage=source_storage,
                    target_storage=target_storage,
                    stats=stats,
                )

    @contextlib.contextmanager
    def get_storage(self) -> Iterator[BaseTransfer[Any]]:
        with self._storage_lock:
            yield self._storage


def copy_items_between(
    keys: Sequence[str],
    *,
    copy_config: DirectCopyConfig | LocalCopyConfig,
    source_storage: BaseTransfer[Any],
    target_storage: BaseTransfer[Any],
    stats: StatsClient | None,
) -> None:
    total_keys = len(keys)
    match copy_config:
        case DirectCopyConfig():
            logger.info("Copying %i keys using the cloud APIs", total_keys)
            emit_direct_copy_progress = (
                functools.partial(emit_copy_progress_metric, stats, copy_config.method) if stats else None
            )
            target_storage.copy_files_from(source=source_storage, keys=keys, progress_fn=emit_direct_copy_progress)
        case LocalCopyConfig():
            logger.info("Copying %i keys by downloading from source/uploading to target", total_keys)
            _copy_via_local_filesystem(
                keys, source=source_storage, target=target_storage, copy_config=copy_config, stats=stats
            )


def _copy_via_local_filesystem(
    keys: Sequence[str],
    *,
    source: BaseTransfer[Any],
    target: BaseTransfer[Any],
    copy_config: LocalCopyConfig,
    stats: StatsClient | None,
) -> None:
    keys_to_copy = len(keys)
    for keys_copied, key in enumerate(keys, start=1):
        with tempfile.TemporaryFile(dir=copy_config.temporary_directory) as temp_file:
            metadata = source.get_contents_to_fileobj(key, temp_file)
            target.store_file_object(key, temp_file, metadata)
        if stats:
            emit_copy_progress_metric(
                stats=stats, copy_method=copy_config.method, completed_files=keys_copied, total_files=keys_to_copy
            )


def emit_copy_progress_metric(stats: StatsClient, copy_method: str, completed_files: int, total_files: int) -> None:
    stats.gauge(
        "astacus_copy_tiered_object_storage_files_remaining",
        total_files - completed_files,
        tags={"copy_method": copy_method},
    )
