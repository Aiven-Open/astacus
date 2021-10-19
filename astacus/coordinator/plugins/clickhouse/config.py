"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from .client import ClickHouseClient, HttpClickHouseClient
from .zookeeper import KazooZooKeeperClient, ZooKeeperClient
from astacus.common.utils import AstacusModel, build_netloc
from typing import List, Optional


class ZooKeeperNode(AstacusModel):
    host: str
    port: int


class ZooKeeperConfiguration(AstacusModel):
    nodes: List[ZooKeeperNode] = []


class ClickHouseNode(AstacusModel):
    host: str
    port: int


class ClickHouseConfiguration(AstacusModel):
    username: Optional[str] = None
    password: Optional[str] = None
    nodes: List[ClickHouseNode] = []


def get_zookeeper_client(configuration: ZooKeeperConfiguration) -> ZooKeeperClient:
    return KazooZooKeeperClient(hosts=[build_netloc(node.host, node.port) for node in configuration.nodes])


def get_clickhouse_clients(configuration: ClickHouseConfiguration) -> List[ClickHouseClient]:
    return [
        HttpClickHouseClient(
            host=node.host,
            port=node.port,
            username=configuration.username,
            password=configuration.password,
        ) for node in configuration.nodes
    ]
