"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from .client import ClickHouseClient, HttpClickHouseClient
from astacus.common.rohmustorage import RohmuStorageConfig
from astacus.common.utils import AstacusModel, build_netloc
from astacus.coordinator.plugins.zookeeper import KazooZooKeeperClient, ZooKeeperClient
from astacus.coordinator.plugins.zookeeper_config import ZooKeeperConfiguration
from collections.abc import Sequence
from pathlib import Path

import enum


class ClickHouseNode(AstacusModel):
    host: str
    port: int


class ClickHouseConfiguration(AstacusModel):
    username: str | None = None
    password: str | None = None
    nodes: Sequence[ClickHouseNode] = []


class ReplicatedDatabaseSettings(AstacusModel):
    max_broken_tables_ratio: float | None = None
    max_replication_lag_to_enqueue: int | None = None
    wait_entry_commited_timeout_sec: int | None = None
    cluster_username: str | None = None
    cluster_password: str | None = None
    cluster_secret: str | None = None
    collection_name: str | None = None


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
    object_storage: DiskObjectStorageConfiguration | None = None


def get_zookeeper_client(configuration: ZooKeeperConfiguration) -> ZooKeeperClient:
    user = configuration.user.to_dataclass() if configuration.user else None
    return KazooZooKeeperClient(hosts=[build_netloc(node.host, node.port) for node in configuration.nodes], user=user)


def get_clickhouse_clients(configuration: ClickHouseConfiguration) -> Sequence[ClickHouseClient]:
    return [
        HttpClickHouseClient(
            host=node.host,
            port=node.port,
            username=configuration.username,
            password=configuration.password,
        )
        for node in configuration.nodes
    ]
