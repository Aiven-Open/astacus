"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from astacus.common.exceptions import NotFoundException
from astacus.common.storage.base import MultiStorage, Storage, StorageUploadResult
from collections.abc import Callable
from pathlib import Path
from typing import BinaryIO, ParamSpec, Self, TypeVar

P = ParamSpec("P")
T = TypeVar("T")


def file_error_wrapper(fun: Callable[P, T]) -> Callable[P, T]:
    """Wrap rohmu exceptions in astacus ones; to be seen what is complete set"""

    def _f(*a: P.args, **kw: P.kwargs) -> T:
        try:
            return fun(*a, **kw)
        except FileNotFoundError as ex:
            raise NotFoundException from ex

    return _f


class FileStorage(Storage):
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)

    @file_error_wrapper
    def download_key_to_file(self, key: str, f: BinaryIO) -> None:
        with open(self.path / key, "rb") as src:
            f.write(src.read())

    def list_key(self) -> list[str]:
        return [p.name for p in self.path.iterdir() if p.is_file()]

    def upload_key_from_file(self, key: str, f: BinaryIO, file_size: int) -> StorageUploadResult:
        path = self.path / key
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path / key, "wb") as dst:
            data = f.read()
            dst.write(data)
        return StorageUploadResult(size=len(data), stored_size=len(data))

    @file_error_wrapper
    def delete_key(self, key: str) -> None:
        (self.path / key).unlink()

    def copy(self) -> Self:
        return self.__class__(self.path)


class FileMultiStorage(MultiStorage):
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def get_storage(self, name: str) -> FileStorage:
        return FileStorage(self.path / name)

    def list_storages(self) -> list[str]:
        return [p.name for p in self.path.iterdir() if p.is_dir()]
