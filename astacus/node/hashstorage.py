"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

"""

from pathlib import Path

import io
import logging
import os

logger = logging.getLogger(__name__)


class HashStorage:
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
        self.download_hexdigest_to_file(hexdigest, open(tempfilename, "wb"))
        os.rename(tempfilename, filename)

    def list_hexdigests(self):
        raise NotImplementedError

    def upload_hexdigest_bytes(self, hexdigest, data):
        self.upload_hexdigest_from_file(hexdigest, io.BytesIO(data))

    def upload_hexdigest_from_file(self, hexdigest, f) -> bool:
        raise NotImplementedError

    def upload_hexdigest_from_path(self, hexdigest, filename):
        self.upload_hexdigest_from_file(hexdigest, open(filename, "rb"))


class FileHashStorage(HashStorage):
    """ Implementation of the hash storage API, which just handles files - primarily useful for testing """
    def __init__(self, path, *, suffix=".dat"):
        self.path = Path(path)
        self.suffix = suffix

    def _hexdigest_to_path(self, hexdigest):
        return self.path / f"{hexdigest}{self.suffix}"

    def delete_hexdigest(self, hexdigest):
        logger.debug("delete_hexdigest %r", hexdigest)
        self._hexdigest_to_path(hexdigest).unlink(missing_ok=True)

    def list_hexdigests(self):
        results = [
            p.stem for p in self.path.iterdir() if p.suffix == self.suffix
        ]
        logger.debug("list_hexdigests => %d", len(results))
        return results

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
