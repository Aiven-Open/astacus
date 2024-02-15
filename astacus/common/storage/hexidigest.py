"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from astacus.common.storage.base import Storage, StorageUploadResult
from pathlib import Path
from typing import BinaryIO, Self

import io
import logging
import os

logger = logging.getLogger(__name__)


class HexDigestStore:
    def __init__(self, storage: Storage) -> None:
        self.storage = storage

    def delete_hexdigest(self, hexdigest: str) -> None:
        self.storage.delete_key(hexdigest)

    def download_hexdigest_bytes(self, hexdigest: str) -> bytes:
        b = io.BytesIO()
        self.download_hexdigest_to_file(hexdigest, b)
        b.seek(0)
        return b.read()

    def download_hexdigest_to_file(self, hexdigest: str, f: BinaryIO) -> None:
        self.storage.download_key_to_file(hexdigest, f)

    def download_hexdigest_to_path(self, hexdigest: str, filename: str | Path) -> None:
        tempfilename = f"{filename}.tmp"
        with open(tempfilename, "wb") as f:
            self.download_hexdigest_to_file(hexdigest, f)
        os.rename(tempfilename, filename)

    def list_hexdigests(self) -> list[str]:
        return self.storage.list_key()

    def upload_hexdigest_bytes(self, hexdigest: str, data: bytes) -> StorageUploadResult:
        return self.upload_hexdigest_from_file(hexdigest, io.BytesIO(data), len(data))

    def upload_hexdigest_from_file(self, hexdigest: str, f: BinaryIO, file_size: int) -> StorageUploadResult:
        return self.storage.upload_key_from_file(hexdigest, f, file_size)

    def copy(self) -> Self:
        return self.__class__(self.storage.copy())
