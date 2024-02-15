"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from astacus.common.storage.base import MultiStorage
from astacus.common.storage.hexidigest import HexDigestStore
from astacus.common.storage.json import JsonStore

import dataclasses


@dataclasses.dataclass
class StorageManager:
    default_storage_name: str
    json_storage: MultiStorage
    hexdigest_storage: MultiStorage

    def get_json_store(self, name: str | None = None) -> JsonStore:
        return JsonStore(self.json_storage.get_storage(self.get_storage_name(name)))

    def get_hexdigest_store(self, name: str | None = None) -> HexDigestStore:
        return HexDigestStore(self.hexdigest_storage.get_storage(self.get_storage_name(name)))

    def get_storage_name(self, name: str | None = None) -> str:
        if name is None or name == "":
            return self.default_storage_name
        return name
