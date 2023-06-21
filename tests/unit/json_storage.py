"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from astacus.common import exceptions
from astacus.common.storage import Json, JsonStorage

import dataclasses
import json


@dataclasses.dataclass(frozen=True)
class MemoryJsonStorage(JsonStorage):
    items: dict[str, str]

    def delete_json(self, name: str) -> None:
        try:
            del self.items[name]
        except KeyError as e:
            raise exceptions.NotFoundException from e

    def download_json(self, name: str) -> Json:
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
