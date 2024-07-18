"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""
from astacus.common.cachingjsonstorage import CachingJsonStorage
from astacus.common.rohmustorage import RohmuConfig, RohmuStorage
from astacus.common.storage import FileStorage, HexDigestStorage, JsonStorage
from astacus.coordinator.state import CoordinatorState
from collections.abc import Iterator, Sequence
from pathlib import Path

import contextlib
import dataclasses
import mmap


@dataclasses.dataclass(frozen=True, kw_only=True)
class StorageFactory:
    storage_config: RohmuConfig
    object_storage_cache: Path | None = None
    state: CoordinatorState | None = None

    def list_storages(self) -> Sequence[str]:
        return sorted(self.storage_config.storages.keys())

    def create_hexdigest_storage(self, storage_name: str | None) -> HexDigestStorage:
        if storage_name is None:
            storage_name = self.storage_config.default_storage
        return RohmuStorage(config=self.storage_config, storage=storage_name)

    def create_json_storage(self, storage_name: str | None) -> JsonStorage:
        if storage_name is None:
            storage_name = self.storage_config.default_storage
        rohmu_storage = RohmuStorage(config=self.storage_config, storage=storage_name)
        if self.object_storage_cache is not None:
            file_storage = FileStorage(path=self.object_storage_cache / storage_name)
            maybe_cached_storage: JsonStorage = CachingJsonStorage(backend_storage=rohmu_storage, cache_storage=file_storage)
        else:
            maybe_cached_storage = rohmu_storage
        if self.state is not None:
            return CacheClearingJsonStorage(state=self.state, storage=maybe_cached_storage)
        return maybe_cached_storage


class CacheClearingJsonStorage(JsonStorage):
    def __init__(self, state: CoordinatorState, storage: JsonStorage) -> None:
        self.state = state
        self.storage = storage

    def close(self) -> None:
        self.storage.close()

    def delete_json(self, name: str) -> None:
        try:
            return self.storage.delete_json(name)
        finally:
            self.state.cached_list_response = None

    @contextlib.contextmanager
    def open_json_bytes(self, name: str) -> Iterator[mmap.mmap]:
        with self.storage.open_json_bytes(name) as json_bytes:
            yield json_bytes

    def list_jsons(self) -> list[str]:
        return self.storage.list_jsons()

    def upload_json_bytes(self, name: str, data: bytes | mmap.mmap) -> bool:
        try:
            return self.storage.upload_json_bytes(name, data)
        finally:
            self.state.cached_list_response = None
