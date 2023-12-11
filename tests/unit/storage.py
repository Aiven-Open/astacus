"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from astacus.common import exceptions
from astacus.common.storage import HexDigestStorage, Json, JsonStorage, StorageUploadResult
from pathlib import Path
from typing import BinaryIO

import dataclasses
import json
import os
import tempfile


class MemoryJsonStorage(JsonStorage):
    def __init__(self, items: dict[str, str]):
        # since this is just used for testing we can be a bit lazy about when
        # the directory is cleaned up and on gc is fine.
        self.dir = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
        self.items = items

    def delete_json(self, name: str) -> None:
        try:
            del self.items[name]
        except KeyError as e:
            raise exceptions.NotFoundException from e

    def download_json(self, name: str) -> Path:
        try:
            data = self.items[name]
        except KeyError as e:
            raise exceptions.NotFoundException from e
        else:
            path = Path(self.dir.name) / name
            with path.open("w") as file:
                file.write(data)
            return path

    def download_and_read_json(self, name: str) -> Json:
        try:
            data = self.items[name]
        except KeyError as e:
            raise exceptions.NotFoundException from e
        else:
            return json.loads(data)

    def list_jsons(self) -> list[str]:
        return sorted(self.items)

    def upload_json_str(self, name: str, data: str) -> bool:
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
