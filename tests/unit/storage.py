"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from astacus.common import exceptions
from astacus.common.storage import HexDigestStorage, JsonStorage, StorageUploadResult
from collections.abc import Iterator
from pathlib import Path
from typing import BinaryIO

import contextlib
import dataclasses
import os


@dataclasses.dataclass(frozen=True)
class MemoryJsonStorage(JsonStorage):
    items: dict[str, bytes]

    def delete_json(self, name: str) -> None:
        try:
            del self.items[name]
        except KeyError as e:
            raise exceptions.NotFoundException from e

    @contextlib.contextmanager
    def open_json_bytes(self, name: str) -> Iterator[bytearray]:
        try:
            data = self.items[name]
        except KeyError as e:
            raise exceptions.NotFoundException from e
        yield bytearray(data)

    def list_jsons(self) -> list[str]:
        return sorted(self.items)

    def upload_json_bytes(self, name: str, data: bytes | bytearray) -> bool:
        if not isinstance(data, bytes):
            data = bytes(data)
        self.items[name] = data
        return True


@dataclasses.dataclass(frozen=True)
class MemoryHexDigestStorage(HexDigestStorage):
    items: dict[str, bytes]

    def delete_hexdigest(self, hexdigest: str) -> None:
        del self.items[hexdigest]

    def download_hexdigest_to_file(self, hexdigest: str, f: BinaryIO) -> bool:
        f.write(self.items[hexdigest])
        return True

    def download_hexdigest_to_path(self, hexdigest: str, filename: str | Path) -> None:
        tempfilename = f"{filename}.tmp"
        with open(tempfilename, "wb") as f:
            self.download_hexdigest_to_file(hexdigest, f)
        os.rename(tempfilename, filename)

    def list_hexdigests(self) -> list[str]:
        return list(self.items.keys())

    def upload_hexdigest_from_file(self, hexdigest: str, f: BinaryIO, file_size: int) -> StorageUploadResult:
        self.items[hexdigest] = f.read(file_size)
        return StorageUploadResult(size=file_size, stored_size=file_size)
