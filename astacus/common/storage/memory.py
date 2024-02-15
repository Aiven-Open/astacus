"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

# Used in tests.
from astacus.common.exceptions import NotFoundException
from astacus.common.storage.base import Storage, StorageUploadResult
from typing import BinaryIO, Self


class MemoryStorage(Storage):
    def __init__(self, items: dict[str, bytes] | None = None) -> None:
        if items is None:
            items = {}
        self.items = items

    def download_key_to_file(self, key: str, f: BinaryIO) -> None:
        if key not in self.items:
            raise NotFoundException(f"Key {key} not found")
        f.write(self.items[key])

    def list_key(self) -> list[str]:
        return list(self.items)

    def upload_key_from_file(self, key: str, f: BinaryIO, file_size: int) -> StorageUploadResult:
        data = f.read()
        self.items[key] = data
        return StorageUploadResult(size=len(data), stored_size=len(data))

    def delete_key(self, key: str) -> None:
        if key not in self.items:
            raise NotFoundException(f"Key {key} not found")
        del self.items[key]

    def copy(self) -> Self:
        return self.__class__()
