"""Copyright (c) 2020 Aiven Ltd
See LICENSE for details.

"""

from .exceptions import NotFoundException
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager
from pathlib import Path
from rohmu.typing import FileLike
from typing import BinaryIO, ParamSpec, TypeAlias, TypeVar

import contextlib
import io
import logging
import mmap
import msgspec
import os
import threading

logger = logging.getLogger(__name__)

P = ParamSpec("P")
T = TypeVar("T")
ST = TypeVar("ST", bound=msgspec.Struct)
JsonArray: TypeAlias = list["Json"]
JsonObject: TypeAlias = dict[str, "Json"]
JsonScalar: TypeAlias = str | int | float | None
Json: TypeAlias = JsonScalar | JsonObject | JsonArray


class StorageUploadResult(msgspec.Struct, kw_only=True, frozen=True):
    size: int
    stored_size: int


class HexDigestStorage(ABC):
    @abstractmethod
    def close(self) -> None: ...

    @abstractmethod
    def delete_hexdigest(self, hexdigest: str) -> None: ...

    def download_hexdigest_bytes(self, hexdigest: str) -> bytes:
        b = io.BytesIO()
        self.download_hexdigest_to_file(hexdigest, b)
        b.seek(0)
        return b.read()

    @abstractmethod
    def download_hexdigest_to_file(self, hexdigest: str, f: FileLike) -> bool: ...

    def download_hexdigest_to_path(self, hexdigest: str, filename: str | Path) -> None:
        tempfilename = f"{filename}.tmp"
        with open(tempfilename, "wb") as f:
            self.download_hexdigest_to_file(hexdigest, f)
        os.rename(tempfilename, filename)

    @abstractmethod
    def list_hexdigests(self) -> list[str]: ...

    def upload_hexdigest_bytes(self, hexdigest: str, data: bytes) -> StorageUploadResult:
        return self.upload_hexdigest_from_file(hexdigest, io.BytesIO(data), len(data))

    @abstractmethod
    def upload_hexdigest_from_file(self, hexdigest: str, f: BinaryIO, file_size: int) -> StorageUploadResult: ...


class JsonStorage(ABC):
    @abstractmethod
    def close(self) -> None:
        pass

    @abstractmethod
    def delete_json(self, name: str) -> None: ...

    @abstractmethod
    def open_json_bytes(self, name: str) -> AbstractContextManager[mmap.mmap]: ...

    @abstractmethod
    def list_jsons(self) -> list[str]: ...

    @abstractmethod
    def upload_json_bytes(self, name: str, data: bytes | mmap.mmap) -> bool: ...

    def upload_json(self, name: str, data: msgspec.Struct) -> bool:
        json_bytes = msgspec.json.encode(data)
        return self.upload_json_bytes(name, json_bytes)

    def download_json(self, name, struct_type: type[ST]) -> ST:
        with self.open_json_bytes(name) as mapped_file:
            # msgspec accepts mmap objects but the type stub does not document that
            return msgspec.json.decode(mapped_file, type=struct_type)  # type: ignore[call-overload]


class Storage(HexDigestStorage, JsonStorage, ABC):
    # This is abstract class which has whatever APIs necessary. Due to that,
    # it is expected not to implement the abstract methods.
    @abstractmethod
    def copy(self) -> "Storage": ...


def file_error_wrapper(fun: Callable[P, T]) -> Callable[P, T]:
    """Wrap rohmu exceptions in astacus ones; to be seen what is complete set."""

    def _f(*a: P.args, **kw: P.kwargs) -> T:
        try:
            return fun(*a, **kw)
        except FileNotFoundError as ex:
            raise NotFoundException from ex

    return _f


class FileStorage(Storage):
    """Implementation of the storage API, which just handles files - primarily useful for testing."""

    def __init__(self, path: str | Path, *, hexdigest_suffix: str = ".dat", json_suffix: str = ".json") -> None:
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)
        self.hexdigest_suffix = hexdigest_suffix
        self.json_suffix = json_suffix

    def close(self) -> None:
        pass

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
    def download_hexdigest_to_file(self, hexdigest: str, f: FileLike) -> bool:
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

    @contextlib.contextmanager
    def open_json_bytes(self, name: str) -> Iterator[mmap.mmap]:
        logger.info("open_json_bytes %r", name)
        path = self._json_to_path(name)
        try:
            with open(path, "rb") as f:
                with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mapped_file:
                    yield mapped_file
        except FileNotFoundError as ex:
            raise NotFoundException from ex

    def list_jsons(self) -> list[str]:
        return self._list(self.json_suffix)

    def upload_json_bytes(self, name: str, data: bytes | mmap.mmap) -> bool:
        logger.info("upload_json_bytes %r", name)
        path = self._json_to_path(name)
        with path.open(mode="wb") as f:
            f.write(data)
        return True


class ThreadLocalStorage:
    def __init__(self, *, storage: Storage) -> None:
        self.threadlocal = threading.local()
        self.storage = storage
        self.local_storages: list[Storage] = []
        self.local_storages_lock = threading.Lock()

    def get_storage(self) -> Storage:
        local_storage = getattr(self.threadlocal, "storage", None)
        if local_storage is None:
            local_storage = self.storage.copy()
            with self.local_storages_lock:
                self.local_storages.append(local_storage)
            setattr(self.threadlocal, "storage", local_storage)
        else:
            assert isinstance(local_storage, Storage)
        return local_storage

    def close(self) -> None:
        for local_storage in self.local_storages:
            local_storage.close()
        self.local_storages.clear()
