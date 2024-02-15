"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from astacus.common.storage.base import Storage
from astacus.common.utils import AstacusModel
from typing import Self, TypeAlias

import io
import logging

try:
    import ujson as json
except ImportError:
    import json  # type: ignore[no-redef]

logger = logging.getLogger(__name__)

JsonArray: TypeAlias = list["Json"]
JsonObject: TypeAlias = dict[str, "Json"]
JsonScalar: TypeAlias = str | int | float | None
Json: TypeAlias = JsonScalar | JsonObject | JsonArray


class JsonStore:
    def __init__(self, storage: Storage) -> None:
        self.storage = storage

    def delete_json(self, name: str) -> None:
        self.storage.delete_key(name)

    def download_json(self, name: str) -> Json:
        f = io.BytesIO()
        self.storage.download_key_to_file(name, f)
        f.seek(0)
        return json.load(f)

    def list_jsons(self) -> list[str]:
        return self.storage.list_key()

    def upload_json_str(self, name: str, data: str) -> bool:
        f = io.BytesIO()
        encoded_data = data.encode()
        f.write(encoded_data)
        f.seek(0)
        self.storage.upload_key_from_file(name, f, len(encoded_data))
        return True

    def upload_json(self, name: str, data: AstacusModel | Json) -> bool:
        if isinstance(data, AstacusModel):
            text = data.json()
        else:
            text = json.dumps(data)
        return self.upload_json_str(name, text)

    def copy(self) -> Self:
        return self.__class__(self.storage.copy())
