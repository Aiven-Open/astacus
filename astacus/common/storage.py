"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

"""

from .exceptions import NotFoundException
from .utils import AstacusModel
from pathlib import Path

import io
import json
import logging
import os
import threading

logger = logging.getLogger(__name__)


class StorageUploadResult(AstacusModel):
    size: int
    stored_size: int


class HexDigestStorage:
    def copy(self):
        raise NotImplementedError

    def delete_hexdigest(self, hexdigest):
        raise NotImplementedError

    def download_hexdigest_bytes(self, hexdigest):
        b = io.BytesIO()
        self.download_hexdigest_to_file(hexdigest, b)
        b.seek(0)
        return b.read()

    def download_hexdigest_to_file(self, hexdigest, f) -> bool:
        raise NotImplementedError

    def download_hexdigest_to_path(self, hexdigest, filename):
        tempfilename = f"{filename}.tmp"
        with open(tempfilename, "wb") as f:
            self.download_hexdigest_to_file(hexdigest, f)
        os.rename(tempfilename, filename)

    def list_hexdigests(self):
        raise NotImplementedError

    def upload_hexdigest_bytes(self, hexdigest, data) -> StorageUploadResult:
        return self.upload_hexdigest_from_file(hexdigest, io.BytesIO(data))

    def upload_hexdigest_from_file(self, hexdigest, f) -> StorageUploadResult:
        raise NotImplementedError

    def upload_hexdigest_from_path(self, hexdigest, filename) -> StorageUploadResult:
        with open(filename, "rb") as f:
            return self.upload_hexdigest_from_file(hexdigest, f)


class JsonStorage:
    def delete_json(self, name: str):
        raise NotImplementedError

    def download_json(self, name: str):
        raise NotImplementedError

    def list_jsons(self):
        raise NotImplementedError

    def upload_json(self, name: str, data):
        if isinstance(data, AstacusModel):
            text = data.json()
        else:
            text = json.dumps(data)
        return self.upload_json_str(name, text)

    def upload_json_str(self, name: str, data: str):
        raise NotImplementedError


class Storage(HexDigestStorage, JsonStorage):
    # pylint: disable=abstract-method
    # This is abstract class which has whatever APIs necessary. Due to that,
    # it is expected not to implement the abstract methods.
    pass


def file_error_wrapper(fun):
    """Wrap rohmu exceptions in astacus ones; to be seen what is complete set"""

    def _f(*a, **kw):
        try:
            return fun(*a, **kw)
        except FileNotFoundError as ex:
            raise NotFoundException from ex

    return _f


class FileStorage(Storage):
    """Implementation of the storage API, which just handles files - primarily useful for testing"""

    def __init__(self, path, *, hexdigest_suffix=".dat", json_suffix=".json"):
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)
        self.hexdigest_suffix = hexdigest_suffix
        self.json_suffix = json_suffix

    def copy(self):
        return FileStorage(path=self.path, hexdigest_suffix=self.hexdigest_suffix, json_suffix=self.json_suffix)

    def _hexdigest_to_path(self, hexdigest):
        return self.path / f"{hexdigest}{self.hexdigest_suffix}"

    def _json_to_path(self, name):
        return self.path / f"{name}{self.json_suffix}"

    @file_error_wrapper
    def delete_hexdigest(self, hexdigest):
        logger.debug("delete_hexdigest %r", hexdigest)
        self._hexdigest_to_path(hexdigest).unlink()

    def _list(self, suffix):
        results = [p.stem for p in self.path.iterdir() if p.suffix == suffix]
        logger.debug("_list %s => %d", suffix, len(results))
        return results

    def list_hexdigests(self):
        return self._list(self.hexdigest_suffix)

    @file_error_wrapper
    def download_hexdigest_to_file(self, hexdigest, f) -> bool:
        logger.debug("download_hexdigest_to_file %r", hexdigest)
        path = self._hexdigest_to_path(hexdigest)
        f.write(path.read_bytes())
        return True

    def upload_hexdigest_from_file(self, hexdigest, f) -> StorageUploadResult:
        logger.debug("upload_hexdigest_from_file %r", hexdigest)
        path = self._hexdigest_to_path(hexdigest)
        data = f.read()
        path.write_bytes(data)
        return StorageUploadResult(size=len(data), stored_size=len(data))

    @file_error_wrapper
    def delete_json(self, name: str):
        logger.debug("delete_json %r", name)
        self._json_to_path(name).unlink()

    @file_error_wrapper
    def download_json(self, name: str):
        logger.debug("download_json %r", name)
        path = self._json_to_path(name)
        with open(path) as f:
            return json.load(f)

    def list_jsons(self):
        return self._list(self.json_suffix)

    def upload_json_str(self, name: str, data: str):
        logger.debug("upload_json_str %r", name)
        path = self._json_to_path(name)
        with path.open(mode="w") as f:
            f.write(data)


class MultiStorage:
    def get_default_storage(self):
        return self.get_storage(self.get_default_storage_name())

    def get_default_storage_name(self):
        raise NotImplementedError

    def get_storage(self, name):
        raise NotImplementedError

    def list_storages(self):
        raise NotImplementedError


class MultiFileStorage(MultiStorage):
    def __init__(self, path, **kw):
        self.path = Path(path)
        self.kw = kw
        self._storages = set()

    def get_storage(self, name):
        self._storages.add(name)
        return FileStorage(self.path / name, **self.kw)

    def get_default_storage_name(self):
        return sorted(self._storages)[-1]

    def list_storages(self):
        return sorted(self._storages)


class ThreadLocalStorage:
    def __init__(self, *, storage: Storage):
        self.threadlocal = threading.local()
        self.storage = storage

    @property
    def local_storage(self):
        local_storage = getattr(self.threadlocal, "storage", None)
        if local_storage is None:
            local_storage = self.storage.copy()
            setattr(self.threadlocal, "storage", local_storage)
        return local_storage
