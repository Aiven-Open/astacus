"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from astacus.common.exceptions import NotFoundException
from astacus.common.storage.base import MultiStorage, Storage
from typing import BinaryIO, Self


class CachedStorage(Storage):
    def __init__(self, storage: Storage, cache: Storage):
        self.storage = storage
        self.cache = cache
        self._backend_json_set_cache: set[str] | None = None
        self._backend_json_list: list[str] | None = None

    def download_key_to_file(self, key: str, f: BinaryIO) -> None:
        if key not in self._backend_json_set:
            raise NotFoundException()
        try:
            self.cache.download_key_to_file(key, f)
        except NotFoundException:
            self.storage.download_key_to_file(key, f)

    def list_key(self) -> list[str]:
        if self._backend_json_list is None:
            self._backend_json_list = sorted(self._backend_json_set)
        return self._backend_json_list

    def upload_key_from_file(self, key: str, f: BinaryIO, file_size: int):
        self.storage.upload_key_from_file(key, f, file_size)
        f.seek(0)
        self.cache.upload_key_from_file(key, f, file_size)
        self._backend_json_set_add(key)

    def delete_key(self, key: str):
        self.storage.delete_key(key)
        self.cache.delete_key(key)
        self._backend_json_set_remove(key)

    def copy(self) -> Self:
        return self.__class__(self.storage.copy(), self.cache.copy())

    @property
    def _backend_json_set(self) -> set[str]:
        if self._backend_json_set_cache is None:
            self._backend_json_set_cache = set(self.storage.list_key())
        return self._backend_json_set_cache

    def _backend_json_set_add(self, x: str) -> None:
        self._backend_json_set.add(x)
        self._backend_json_list = None

    def _backend_json_set_remove(self, x: str) -> None:
        self._backend_json_set.remove(x)
        self._backend_json_list = None


class CachedMultiStorage(MultiStorage):
    def __init__(self, storage: MultiStorage, cache: MultiStorage):
        self.storage_factory = storage
        self.cache_factory = cache

    def get_storage(self, name) -> Storage:
        return CachedStorage(self.storage_factory.get_storage(name), self.cache_factory.get_storage(name))

    def list_storages(self) -> list[str]:
        return self.storage_factory.list_storages()
