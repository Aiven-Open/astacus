"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from abc import ABC, abstractmethod
from astacus.common.utils import AstacusModel
from typing import BinaryIO, Self


class StorageUploadResult(AstacusModel):
    size: int
    stored_size: int


class Storage(ABC):
    """Simple k-v object storage interface."""

    # raises astacus.common.exceptions.NotFoundException
    @abstractmethod
    def download_key_to_file(self, key: str, f: BinaryIO) -> None:
        ...

    @abstractmethod
    def list_key(self) -> list[str]:
        ...

    @abstractmethod
    def upload_key_from_file(self, key: str, f: BinaryIO, file_size: int) -> StorageUploadResult:
        ...

    # raises astacus.common.exceptions.NotFoundException
    @abstractmethod
    def delete_key(self, key: str) -> None:
        ...

    @abstractmethod
    def copy(self) -> Self:
        ...


class MultiStorage(ABC):
    @abstractmethod
    def get_storage(self, name: str) -> Storage:
        ...

    @abstractmethod
    def list_storages(self) -> list[str]:
        ...
