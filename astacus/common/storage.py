"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

"""
from .exceptions import NotFoundException
from .utils import AstacusModel
from abc import ABC, abstractmethod
from pathlib import Path
from typing import BinaryIO, Callable, Generic, ParamSpec, TypeAlias, TypeVar

import io

try:
    import ujson as json  # type: ignore [import]
except ImportError:
    import json

import logging
import os
import threading

logger = logging.getLogger(__name__)

P = ParamSpec("P")
T = TypeVar("T")
JsonArray: TypeAlias = list["Json"]
JsonObject: TypeAlias = dict[str, "Json"]
JsonScalar: TypeAlias = str | int | float | None
Json: TypeAlias = JsonScalar | JsonObject | JsonArray


class StorageUploadResult(AstacusModel):
    size: int
    stored_size: int


class HexDigestStorage(ABC):
    @abstractmethod
    def delete_hexdigest(self, hexdigest: str) -> None:
        ...

    def download_hexdigest_bytes(self, hexdigest: str) -> bytes:
        b = io.BytesIO()
        self.download_hexdigest_to_file(hexdigest, b)
        b.seek(0)
        return b.read()

    @abstractmethod
    def download_hexdigest_to_file(self, hexdigest: str, f: BinaryIO) -> bool:
        ...

    def download_hexdigest_to_path(self, hexdigest: str, filename: str | Path) -> None:
        tempfilename = f"{filename}.tmp"
        with open(tempfilename, "wb") as f:
            self.download_hexdigest_to_file(hexdigest, f)
        os.rename(tempfilename, filename)

    @abstractmethod
    def list_hexdigests(self) -> list[str]:
        ...

    def upload_hexdigest_bytes(self, hexdigest: str, data: bytes) -> StorageUploadResult:
        return self.upload_hexdigest_from_file(hexdigest, io.BytesIO(data), len(data))

    @abstractmethod
    def upload_hexdigest_from_file(self, hexdigest: str, f: BinaryIO, file_size: int) -> StorageUploadResult:
        ...


class JsonStorage(ABC):
    @abstractmethod
    def delete_json(self, name: str) -> None:
        ...

    @abstractmethod
    def download_json(self, name: str) -> Path:
        ...

    @abstractmethod
    def download_and_read_json(self, name: str) -> Json:
        ...

    @abstractmethod
    def list_jsons(self) -> list[str]:
        ...

    @abstractmethod
    def upload_json_str(self, name: str, data: str) -> bool:
        ...

    def upload_json(self, name: str, data: AstacusModel | Json) -> bool:
        if isinstance(data, AstacusModel):
            text = data.json()
        else:
            text = json.dumps(data)
        return self.upload_json_str(name, text)


class Storage(HexDigestStorage, JsonStorage, ABC):
    # pylint: disable=abstract-method
    # This is abstract class which has whatever APIs necessary. Due to that,
    # it is expected not to implement the abstract methods.
    @abstractmethod
    def copy(self) -> "Storage":
        ...


def file_error_wrapper(fun: Callable[P, T]) -> Callable[P, T]:
    """Wrap rohmu exceptions in astacus ones; to be seen what is complete set"""

    def _f(*a: P.args, **kw: P.kwargs) -> T:
        try:
            return fun(*a, **kw)
        except FileNotFoundError as ex:
            raise NotFoundException from ex

    return _f


class FileStorage(Storage):
    """Implementation of the storage API, which just handles files - primarily useful for testing"""

    def __init__(self, path: str | Path, *, hexdigest_suffix: str = ".dat", json_suffix: str = ".json") -> None:
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)
        self.hexdigest_suffix = hexdigest_suffix
        self.json_suffix = json_suffix

    def copy(self) -> "FileStorage":
        return FileStorage(path=self.path, hexdigest_suffix=self.hexdigest_suffix, json_suffix=self.json_suffix)

    def _hexdigest_to_path(self, hexdigest: str) -> Path:
        return self.path / f"{hexdigest}{self.hexdigest_suffix}"

    def _json_to_path(self, name: str) -> Path:
        return self.path / f"{name}{self.json_suffix}"

    @file_error_wrapper
    def delete_hexdigest(self, hexdigest: str) -> None:
        logger.info("delete_hexdigest %r", hexdigest)
        self._hexdigest_to_path(hexdigest).unlink()

    def _list(self, suffix: str) -> list[str]:
        results = [p.stem for p in self.path.iterdir() if p.suffix == suffix]
        logger.info("_list %s => %d", suffix, len(results))
        return results

    def list_hexdigests(self) -> list[str]:
        return self._list(self.hexdigest_suffix)

    @file_error_wrapper
    def download_hexdigest_to_file(self, hexdigest: str, f: BinaryIO) -> bool:
        logger.info("download_hexdigest_to_file %r", hexdigest)
        path = self._hexdigest_to_path(hexdigest)
        f.write(path.read_bytes())
        return True

    def upload_hexdigest_from_file(self, hexdigest: str, f: BinaryIO, file_size: int) -> StorageUploadResult:
        logger.info("upload_hexdigest_from_file %r", hexdigest)
        path = self._hexdigest_to_path(hexdigest)
        data = f.read()
        path.write_bytes(data)
        return StorageUploadResult(size=len(data), stored_size=len(data))

    @file_error_wrapper
    def delete_json(self, name: str) -> None:
        logger.info("delete_json %r", name)
        self._json_to_path(name).unlink()

    @file_error_wrapper
    def download_json(self, name: str) -> Path:
        logger.info("download_json %r", name)
        path = self._json_to_path(name)
        if not path.exists():
            raise NotFoundException
        return path

    @file_error_wrapper
    def download_and_read_json(self, name: str) -> Json:
        logger.info("download_and_read_json %r", name)
        path = self._json_to_path(name)
        with open(path) as f:
            return json.load(f)

    def list_jsons(self) -> list[str]:
        return self._list(self.json_suffix)

    def upload_json_str(self, name: str, data: str) -> bool:
        logger.info("upload_json_str %r", name)
        path = self._json_to_path(name)
        with path.open(mode="w") as f:
            f.write(data)
        return True


class MultiStorage(Generic[T]):
    def get_default_storage(self) -> T:
        return self.get_storage(self.get_default_storage_name())

    def get_default_storage_name(self) -> str:
        raise NotImplementedError

    def get_storage(self, name: str) -> T:
        raise NotImplementedError

    def list_storages(self) -> list[str]:
        raise NotImplementedError


class MultiFileStorage(MultiStorage[FileStorage]):
    def __init__(self, path, **kw):
        self.path = Path(path)
        self.kw = kw
        self._storages = set()

    def get_storage(self, name: str) -> FileStorage:
        self._storages.add(name)
        return FileStorage(self.path / name, **self.kw)

    def get_default_storage_name(self) -> str:
        return sorted(self._storages)[-1]

    def list_storages(self) -> list[str]:
        return sorted(self._storages)


class ThreadLocalStorage:
    def __init__(self, *, storage: Storage) -> None:
        self.threadlocal = threading.local()
        self.storage = storage

    @property
    def local_storage(self) -> Storage:
        local_storage = getattr(self.threadlocal, "storage", None)
        if local_storage is None:
            local_storage = self.storage.copy()
            setattr(self.threadlocal, "storage", local_storage)
        else:
            assert isinstance(local_storage, Storage)
        return local_storage
