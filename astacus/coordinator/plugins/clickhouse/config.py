"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from .client import ClickHouseClient, HttpClickHouseClient
from astacus.common.rohmustorage import RohmuStorageConfig
from astacus.common.utils import AstacusModel, build_netloc
from astacus.coordinator.plugins.zookeeper import KazooZooKeeperClient, ZooKeeperClient
from astacus.coordinator.plugins.zookeeper_config import ZooKeeperConfiguration
from pathlib import Path
from typing import List, Optional

import enum


class ClickHouseNode(AstacusModel):
    host: str
    port: int


class ClickHouseConfiguration(AstacusModel):
    username: Optional[str] = None
    password: Optional[str] = None
    nodes: List[ClickHouseNode] = []


class ReplicatedDatabaseSettings(AstacusModel):
    max_broken_tables_ratio: Optional[float]
    max_replication_lag_to_enqueue: Optional[int]
    wait_entry_commited_timeout_sec: Optional[int]
    cluster_username: Optional[str]
    cluster_password: Optional[str]
    cluster_secret: Optional[str]
    collection_name: Optional[str]


class DiskType(enum.Enum):
    local = "local"
    object_storage = "object_storage"


class DiskObjectStorageConfiguration(AstacusModel):
    default_storage: str
    storages: dict[str, RohmuStorageConfig]


class DiskConfiguration(AstacusModel):
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
