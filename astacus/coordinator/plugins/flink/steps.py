"""
Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""

from astacus.common.exceptions import TransientException
from astacus.coordinator.cluster import Cluster
from astacus.coordinator.plugins.base import BackupManifestStep, Step, StepsContext
from astacus.coordinator.plugins.flink.manifest import FlinkManifest
from astacus.coordinator.plugins.zookeeper import ChangeWatch, ZooKeeperClient, ZooKeeperConnection
from typing import Any, Dict, List

import dataclasses
import logging

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class RetrieveDataStep(Step[Dict[str, Any]]):
    """
    Backups Flink tables from ZooKeeper.
    """
    zookeeper_client: ZooKeeperClient
    zookeeper_paths: List[str]

    async def run_step(self, cluster: Cluster, context: StepsContext) -> Dict[str, Any]:
        async with self.zookeeper_client.connect() as connection:
            change_watch = ChangeWatch()
            results: Dict[str, Any] = {}
            for zk_path in self.zookeeper_paths:
                result = await self._get_result(zk_path, connection, change_watch)
                if result:
                    results.update(result)
            if change_watch.has_changed:
                raise TransientException("Concurrent modification during access entities retrieval")
        return results

    async def _get_result(self, zk_path: str, connection: ZooKeeperConnection, change_watch: ChangeWatch) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        if await connection.exists(zk_path):
            result[zk_path] = await self._get_data_recursively(
                zk_prefix=zk_path, current_node=zk_path, connection=connection, watch=change_watch
            )
        return result

    async def _get_data_recursively(
        self, zk_prefix: str, current_node: str, connection: ZooKeeperConnection, watch: ChangeWatch
    ) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        if await connection.exists(zk_prefix):
            children = await connection.get_children(current_node, watch=watch)
            for child in children:
                value = await connection.get(f"{current_node}/{child}", watch=watch)
                if value:
                    result[child] = value.decode("UTF-8")
                else:
                    result[child] = await self._get_data_recursively(zk_prefix, f"{current_node}/{child}", connection, watch)
        return result


@dataclasses.dataclass
class CreateFlinkManifestStep(Step[FlinkManifest]):
    """
    Collects data from previous steps into a `FlinkManifest`.
    """
    async def run_step(self, cluster: Cluster, context: StepsContext) -> FlinkManifest:
        return FlinkManifest(data=context.get_result(RetrieveDataStep))


@dataclasses.dataclass
class FlinkManifestStep(Step[FlinkManifest]):
    """
    Extracts the Flink plugin manifest from the main backup manifest.
    """
    async def run_step(self, cluster: Cluster, context: StepsContext) -> FlinkManifest:
        backup_manifest = context.get_result(BackupManifestStep)
        return FlinkManifest.parse_obj(backup_manifest.plugin_data)


@dataclasses.dataclass
class RestoreDataStep(Step[None]):
    """
    Restores Flint tables to ZooKeeper.
    """
    zookeeper_client: ZooKeeperClient
    zookeeper_paths: List[str]

    async def run_step(self, cluster: Cluster, context: StepsContext) -> None:
        flink_manifest = context.get_result(FlinkManifestStep)
        if flink_manifest.data:
            async with self.zookeeper_client.connect() as connection:
                for zk_path in self.zookeeper_paths:
                    await self._restore_data(connection, zk_path, flink_manifest.data, zk_path)

    async def _restore_data(self, connection: ZooKeeperConnection, zk_path: str, data: Dict, current_node_path: str) -> None:
        current_node = data.get(current_node_path)
        if isinstance(current_node, dict):
            await connection.create(zk_path, bytes())
            for key in current_node.keys():
                await self._restore_data(connection, f"{zk_path}/{key}", current_node, key)
        elif current_node:
            await connection.create(zk_path, current_node.encode("UTF-8"))
