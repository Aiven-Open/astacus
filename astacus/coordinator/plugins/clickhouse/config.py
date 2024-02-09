"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from .client import ClickHouseClient, HttpClickHouseClient
from astacus.common.rohmustorage import RohmuStorageConfig
from astacus.common.utils import build_netloc
from astacus.coordinator.plugins.zookeeper import KazooZooKeeperClient, ZooKeeperClient
from astacus.coordinator.plugins.zookeeper_config import ZooKeeperConfiguration
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import List, Optional

import enum
import msgspec


class ClickHouseNode(msgspec.Struct, kw_only=True, frozen=True):
    host: str
    port: int


class ClickHouseConfiguration(msgspec.Struct, kw_only=True, frozen=True):
    username: Optional[str] = None
    password: Optional[str] = None
    nodes: Sequence[ClickHouseNode] = msgspec.field(default_factory=list)


class ReplicatedDatabaseSettings(msgspec.Struct, kw_only=True, frozen=True):
    max_broken_tables_ratio: Optional[float] = None
    max_replication_lag_to_enqueue: Optional[int] = None
    wait_entry_commited_timeout_sec: Optional[int] = None
    cluster_username: Optional[str] = None
    cluster_password: Optional[str] = None
    cluster_secret: Optional[str] = None
    collection_name: Optional[str] = None


class DiskType(enum.Enum):
    local = "local"
    object_storage = "object_storage"


class DiskObjectStorageConfiguration(msgspec.Struct, kw_only=True, frozen=True):
    default_storage: str
    storages: Mapping[str, RohmuStorageConfig]


class DiskConfiguration(msgspec.Struct, kw_only=True, frozen=True):
    class Config:
        use_enum_values = False

    type: DiskType
    path: Path
    name: str
    object_storage: Optional[DiskObjectStorageConfiguration] = None


def get_zookeeper_client(configuration: ZooKeeperConfiguration) -> ZooKeeperClient:
    user = configuration.user.to_dataclass() if configuration.user else None
    return KazooZooKeeperClient(hosts=[build_netloc(node.host, node.port) for node in configuration.nodes], user=user)


def get_clickhouse_clients(configuration: ClickHouseConfiguration) -> List[ClickHouseClient]:
    return [
        HttpClickHouseClient(
            host=node.host,
            port=node.port,
            username=configuration.username,
            password=configuration.password,
        )
        for node in configuration.nodes
    ]
