"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details
"""

from astacus.common.storage.base import MultiStorage
from astacus.common.storage.hexidigest import HexDigestStore
from astacus.common.storage.json import JsonStore
from astacus.common.storage.node import NodeManifestStore
from pathlib import Path

import dataclasses


@dataclasses.dataclass
class StorageManager:
    n_nodes: int
    default_storage_name: str
    json_storage: MultiStorage
    hexdigest_storage: MultiStorage
    node_storage: MultiStorage
    tmp_path: Path

    def get_json_store(self, name: str | None = None) -> JsonStore:
        return JsonStore(self.json_storage.get_storage(self.get_storage_name(name)))

    def get_hexdigest_store(self, name: str | None = None) -> HexDigestStore:
        return HexDigestStore(self.hexdigest_storage.get_storage(self.get_storage_name(name)))

    def get_storage_name(self, name: str | None = None) -> str:
        if name is None or name == "":
            return self.default_storage_name
        return name

    def get_node_store(self, node_id: int, name: str | None = None) -> NodeManifestStore:
        return NodeManifestStore(self.node_storage.get_storage(self.get_storage_name(name)), node_id, self.tmp_path)

    def get_node_stores(self, name: str | None = None) -> list[NodeManifestStore]:
        return [self.get_node_store(node_id, name) for node_id in range(self.n_nodes)]
