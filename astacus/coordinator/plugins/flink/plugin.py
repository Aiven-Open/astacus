"""Copyright (c) 2022 Aiven Ltd
See LICENSE for details

Flink backup/restore plugin

"""
from astacus.common import ipc
from astacus.common.ipc import Plugin
from astacus.coordinator.plugins.base import (
    BackupManifestStep,
    BackupNameStep,
    CoordinatorPlugin,
    OperationContext,
    Step,
    UploadManifestStep,
)
from astacus.coordinator.plugins.flink.steps import (
    FlinkManifestStep,
    PrepareFlinkManifestStep,
    RestoreDataStep,
    RetrieveDataStep,
)
from astacus.coordinator.plugins.zookeeper_config import get_zookeeper_client, ZooKeeperConfiguration
from typing import Any, List, Sequence

import logging

logger = logging.getLogger(__name__)


class FlinkPlugin(CoordinatorPlugin):
    zookeeper: ZooKeeperConfiguration = ZooKeeperConfiguration()
    zookeeper_paths: List[str] = ["/catalog"]

    def get_backup_steps(self, *, context: OperationContext) -> Sequence[Step[Any]]:
        zookeeper_client = get_zookeeper_client(self.zookeeper)
        return [
            RetrieveDataStep(zookeeper_client=zookeeper_client, zookeeper_paths=self.zookeeper_paths),
            PrepareFlinkManifestStep(),
            UploadManifestStep(
                json_storage=context.json_storage,
                plugin=Plugin.flink,
                plugin_manifest_step=PrepareFlinkManifestStep,
                snapshot_step=None,
                upload_step=None,
            ),
        ]

    def get_delta_backup_steps(self, *, context: OperationContext) -> Sequence[Step[Any]]:
        raise NotImplementedError

    def get_restore_steps(self, *, context: OperationContext, req: ipc.RestoreRequest) -> Sequence[Step[Any]]:
        zookeeper_client = get_zookeeper_client(self.zookeeper)
        return [
            BackupNameStep(json_storage=context.json_storage, requested_name=req.name),
            BackupManifestStep(json_storage=context.json_storage),
            FlinkManifestStep(),
            RestoreDataStep(zookeeper_client=zookeeper_client, zookeeper_paths=self.zookeeper_paths),
        ]
