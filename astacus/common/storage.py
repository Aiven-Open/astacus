"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

"""

from .utils import AstacusModel
from pathlib import Path

import io
import json
import logging
import os

logger = logging.getLogger(__name__)


class HexDigestStorage:
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

    def upload_hexdigest_bytes(self, hexdigest, data) -> bool:
        return self.upload_hexdigest_from_file(hexdigest, io.BytesIO(data))

    def upload_hexdigest_from_file(self, hexdigest, f) -> bool:
        raise NotImplementedError

    def upload_hexdigest_from_path(self, hexdigest, filename) -> bool:
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


class FileStorage(Storage):
    """ Implementation of the storage API, which just handles files - primarily useful for testing """
    def __init__(self, path, *, hexdigest_suffix=".dat", json_suffix=".json"):
        self.path = Path(path)
        self.path.mkdir(exist_ok=True)
        self.hexdigest_suffix = hexdigest_suffix
        self.json_suffix = json_suffix

    def _hexdigest_to_path(self, hexdigest):
        return self.path / f"{hexdigest}{self.hexdigest_suffix}"

    def _json_to_path(self, name):
        return self.path / f"{name}{self.json_suffix}"

    def delete_hexdigest(self, hexdigest):
        logger.debug("delete_hexdigest %r", hexdigest)
        self._hexdigest_to_path(hexdigest).unlink()

    def _list(self, suffix):
        results = [p.stem for p in self.path.iterdir() if p.suffix == suffix]
        logger.debug("_list %s => %d", suffix, len(results))
        return results

    def list_hexdigests(self):
        return self._list(self.hexdigest_suffix)

    def download_hexdigest_to_file(self, hexdigest, f) -> bool:
        logger.debug("download_hexdigest_to_file %r", hexdigest)
        path = self._hexdigest_to_path(hexdigest)
        f.write(path.read_bytes())
        return True

    def upload_hexdigest_from_file(self, hexdigest, f) -> bool:
        logger.debug("upload_hexdigest_from_file %r", hexdigest)
        path = self._hexdigest_to_path(hexdigest)
        path.write_bytes(f.read())
        return True

    def delete_json(self, name: str):
        logger.debug("delete_json %r", name)
        self._json_to_path(name).unlink()

    def download_json(self, name: str):
        logger.debug("download_json %r", name)
        path = self._json_to_path(name)
        return json.load(open(path))

    def list_jsons(self):
        return self._list(self.json_suffix)

    def upload_json_str(self, name: str, data: str):
        logger.debug("upload_json_str %r", name)
        path = self._json_to_path(name)
        with path.open(mode="w") as f:
            f.write(data)
