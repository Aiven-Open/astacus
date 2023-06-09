"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Caching layer on top of storage.

It implements only the JsonStorage API.

Underlying storage is used to:

- list jsons (once, when APIs are first used; after that, cluster lock
  should be held so all changes to json storage are authored by us)

- download jsons that are requested and not yet locally stored

- uploads (duh)

the list/download are returned mostly from the cache_storage. The
assumption is that backups are immutable, that is, if file backup-X
exists, its contents stay the same.

"""

from .exceptions import NotFoundException
from .storage import Json, JsonStorage, MultiStorage


class CachingJsonStorage(JsonStorage):
    _backend_json_set_cache = None
    _backend_json_list = None

    def __init__(self, *, backend_storage: JsonStorage, cache_storage: JsonStorage) -> None:
        self.backend_storage = backend_storage
        self.cache_storage = cache_storage

    @property
    def _backend_json_set(self) -> set[str]:
        if self._backend_json_set_cache is None:
            self._backend_json_set_cache = set(self.backend_storage.list_jsons())
        return self._backend_json_set_cache

    def _backend_json_set_add(self, x: str) -> None:
        self._backend_json_set.add(x)
        self._backend_json_list = None

    def _backend_json_set_remove(self, x: str) -> None:
        self._backend_json_set.remove(x)
        self._backend_json_list = None

    def delete_json(self, name: str) -> None:
        if name not in self._backend_json_set:
            raise NotFoundException()
        try:
            self.cache_storage.delete_json(name)
        except NotFoundException:
            pass
        self.backend_storage.delete_json(name)
        self._backend_json_set_remove(name)

    def download_json(self, name: str) -> Json:
        if name not in self._backend_json_set:
            raise NotFoundException()
        try:
            return self.cache_storage.download_json(name)
        except NotFoundException:
            pass
        data = self.backend_storage.download_json(name)
        self.cache_storage.upload_json(name, data)
        return data

    def list_jsons(self) -> list[str]:
        if self._backend_json_list is None:
            self._backend_json_list = sorted(self._backend_json_set)
        return self._backend_json_list

    def upload_json_str(self, name: str, data: str) -> bool:
        self.cache_storage.upload_json_str(name, data)
        self.backend_storage.upload_json_str(name, data)
        self._backend_json_set_add(name)
        return True


class MultiCachingJsonStorage(MultiStorage[CachingJsonStorage]):
    def __init__(self, *, backend_mstorage: MultiStorage, cache_mstorage: MultiStorage) -> None:
        self.cache_mstorage = cache_mstorage
        self.backend_mstorage = backend_mstorage

    def get_storage(self, name: str) -> CachingJsonStorage:
        return CachingJsonStorage(
            backend_storage=self.backend_mstorage.get_storage(name), cache_storage=self.cache_mstorage.get_storage(name)
        )

    def get_default_storage_name(self) -> str:
        return self.backend_mstorage.get_default_storage_name()

    def list_storages(self) -> list[str]:
        return self.backend_mstorage.list_storages()
