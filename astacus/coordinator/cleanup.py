"""
Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Database cleanup operation

"""

from astacus.common import ipc, magic, utils
from astacus.coordinator.cluster import Cluster
from astacus.coordinator.coordinator import Coordinator, LockedCoordinatorOp
from astacus.coordinator.manifest import download_backup_manifest
from fastapi import Depends

import logging

logger = logging.getLogger(__name__)


class CleanupOp(LockedCoordinatorOp):
    def __init__(self, *, c: Coordinator = Depends(), req: ipc.CleanupRequest = ipc.CleanupRequest()):
        super().__init__(c=c)
        self.req = req
        self.retention = c.config.retention
        self.context = c.get_operation_context(requested_storage=req.storage)

    async def run_with_lock(self, cluster: Cluster) -> None:
        retention = self.retention.copy()
        if self.req.retention is not None:
            # This returns only non-defaults -> non-Nones
            for k, v in self.req.retention.dict().items():
                setattr(retention, k, v)
        all_backups = await self._list_backups()
        kept_backups = all_backups.difference(set(self.req.explicit_delete))
        kept_backups = await self.determine_kept_backups(retention=retention, backups=kept_backups)
        await self.delete_backups(all_backups.difference(kept_backups))

    async def _list_backups(self):
        return set(b for b in await self.context.json_storage.list_jsons() if b.startswith(magic.JSON_BACKUP_PREFIX))

    async def _download_backup_manifests(self, backups):
        # Due to rate limiting, it might be better to not do this in parallel
        return [await download_backup_manifest(self.context.json_storage, backup) for backup in backups]

    async def delete_backups(self, backups):
        if not backups:
            logger.debug("delete_backups: nothing to delete")
            return
        for backup in backups:
            logger.info("deleting backup %r", backup)
            await self.context.json_storage.delete_json(backup)
        await self.delete_dangling_hexdigests()

    async def delete_dangling_hexdigests(self):
        logger.info("delete_dangling_hexdigests - downloading backup list")
        backups = await self._list_backups()
        logger.info("downloading backup manifests")
        manifests = await self._download_backup_manifests(backups)
        kept_hexdigests = set()
        for manifest in manifests:
            for result in manifest.snapshot_results:
                assert result.hashes is not None
                kept_hexdigests = kept_hexdigests | set(h.hexdigest for h in result.hashes if h.hexdigest)

        all_hexdigests = await self.context.hexdigest_storage.list_hexdigests()
        extra_hexdigests = set(all_hexdigests).difference(kept_hexdigests)
        if not extra_hexdigests:
            return
        logger.info("deleting %d hexdigests from object storage", len(extra_hexdigests))
        for i, hexdigest in enumerate(extra_hexdigests, 1):
            # Due to rate limiting, it might be better to not do this in parallel
            await self.context.hexdigest_storage.delete_hexdigest(hexdigest)
            if i % 100 == 0:
                self.stats.gauge("astacus_cleanup_hexdigest_progress", i)
                self.stats.gauge("astacus_cleanup_hexdigest_progress_percent", 100.0 * i / len(extra_hexdigests))

    async def determine_kept_backups(self, *, retention, backups):
        if retention.minimum_backups is not None and retention.minimum_backups >= len(backups):
            return backups
        now = utils.now()
        manifests = sorted(await self._download_backup_manifests(backups), key=lambda manifest: manifest.start, reverse=True)
        while manifests:
            if retention.maximum_backups is not None:
                if retention.maximum_backups < len(manifests):
                    manifests.pop()
                    continue

            # Ok, so now we have at most <maximum_backups> (if set) backups

            # Do we have too _few_ backups to delete any more?
            if retention.minimum_backups is not None:
                if retention.minimum_backups >= len(manifests):
                    break

            if retention.keep_days is not None:
                manifest = manifests[-1]
                if (now - manifest.end).days > retention.keep_days:
                    manifests.pop()
                    continue
            # We don't have any other criteria to filter the backup manifests with
            break

        return set(manifest.filename for manifest in manifests)
