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
from .storage import JsonStorage


class CachingJsonStorage(JsonStorage):
    _backend_json_set_cache = None
    _backend_json_list = None

    def __init__(self, *, backend_storage, cache_storage):
        self.backend_storage = backend_storage
        self.cache_storage = cache_storage

    @property
    def _backend_json_set(self):
        if self._backend_json_set_cache is None:
            self._backend_json_set_cache = set(self.backend_storage.list_jsons())
        return self._backend_json_set_cache

    def _backend_json_set_add(self, x):
        self._backend_json_set.add(x)
        self._backend_json_list = None

    def _backend_json_set_remove(self, x):
        self._backend_json_set.remove(x)
        self._backend_json_list = None

    def delete_json(self, name: str):
        if name not in self._backend_json_set:
            raise NotFoundException()
        try:
            self.cache_storage.delete_json(name)
        except NotFoundException:
            pass
        self.backend_storage.delete_json(name)
        self._backend_json_set_remove(name)

    def download_json(self, name: str):
        if name not in self._backend_json_set:
            raise NotFoundException()
        try:
            return self.cache_storage.download_json(name)
        except NotFoundException:
            pass
        data = self.backend_storage.download_json(name)
        self.cache_storage.upload_json(name, data)
        return data

    def list_jsons(self):
        if self._backend_json_list is None:
            self._backend_json_list = sorted(self._backend_json_set)
        return self._backend_json_list

    def upload_json_str(self, name: str, data: str):
        self.cache_storage.upload_json_str(name, data)
        self.backend_storage.upload_json_str(name, data)
        self._backend_json_set_add(name)
