"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

General restore utilities that are product independent.

The basic file restoration steps should be implementable by using the
API of this module with proper parameters.

"""
from .node import NodeOp
from .snapshotter import Snapshotter
from astacus.common import ipc, utils
from astacus.common.progress import Progress
from astacus.common.rohmustorage import RohmuStorage
from astacus.common.storage import JsonStorage, Storage, ThreadLocalStorage
from astacus.common.utils import get_umask
from collections.abc import Callable, Sequence
from pathlib import Path

import base64
import getpass
import logging
import msgspec
import os
import shutil
import subprocess

logger = logging.getLogger(__name__)


class Downloader(ThreadLocalStorage):
    def __init__(
        self, *, dst: Path, snapshotter: Snapshotter, parallel: int, storage: Storage, copy_dst_owner: bool = False
    ) -> None:
        super().__init__(storage=storage)
        self.dst = dst
        self.snapshotter = snapshotter
        self.snapshot = snapshotter.snapshot
        self.parallel = parallel
        self.copy_dst_owner = copy_dst_owner

    def _snapshotfile_already_exists(self, snapshotfile: ipc.SnapshotFile) -> bool:
        existing_snapshotfile = self.snapshot.get_file(snapshotfile.relative_path)
        return existing_snapshotfile is not None and existing_snapshotfile.equals_excluding_mtime(snapshotfile)

    def _download_snapshotfile(self, snapshotfile: ipc.SnapshotFile) -> None:
        if self._snapshotfile_already_exists(snapshotfile):
            return

        relative_path = snapshotfile.relative_path
        download_path = self.dst / relative_path
        download_path.parent.mkdir(parents=True, exist_ok=True, mode=0o770)
        with utils.open_path_with_atomic_rename(download_path) as f:
            if snapshotfile.hexdigest:
                self.local_storage.download_hexdigest_to_file(snapshotfile.hexdigest, f)
            else:
                assert snapshotfile.content_b64 is not None
                f.write(base64.b64decode(snapshotfile.content_b64))
        os.chmod(download_path, 0o660 & ~get_umask())
        os.utime(download_path, ns=(snapshotfile.mtime_ns, snapshotfile.mtime_ns))

    def _download_snapshotfiles_from_storage(self, snapshotfiles: Sequence[ipc.SnapshotFile]) -> None:
        self._download_snapshotfile(snapshotfiles[0])

        # We don't report progress for these, as local copying
        # should be ~instant
        for snapshotfile in snapshotfiles[1:]:
            self._copy_snapshotfile(snapshotfiles[0], snapshotfile)

    def _copy_snapshotfile(self, snapshotfile_src: ipc.SnapshotFile, snapshotfile: ipc.SnapshotFile) -> None:
        if self._snapshotfile_already_exists(snapshotfile):
            return

        src_path = self.dst / snapshotfile_src.relative_path
        dst_path = self.dst / snapshotfile.relative_path
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(src_path, dst_path)
        os.utime(dst_path, ns=(snapshotfile.mtime_ns, snapshotfile.mtime_ns))

    def download_from_storage(
        self,
        *,
        progress: Progress,
        snapshotstate: ipc.SnapshotState,
        still_running_callback: Callable[[], bool] = lambda: True,
    ) -> None:
        hexdigest_to_snapshotfiles: dict[str, list[ipc.SnapshotFile]] = {}
        valid_relative_path_set = set()
        for snapshotfile in snapshotstate.files:
            valid_relative_path_set.add(snapshotfile.relative_path)
            if snapshotfile.hexdigest:
                hexdigest_to_snapshotfiles.setdefault(snapshotfile.hexdigest, []).append(snapshotfile)

        self.snapshotter.perform_snapshot(progress=Progress())
        # TBD: Error checking, what to do if we're told to restore to existing directory?
        progress.start(sum(1 + snapshotfile.file_size for snapshotfile in snapshotstate.files))
        for snapshotfile in snapshotstate.files:
            if not snapshotfile.hexdigest:
                self._download_snapshotfile(snapshotfile)
                progress.download_success(snapshotfile.file_size + 1)
        all_snapshotfiles = hexdigest_to_snapshotfiles.values()

        def _cb(*, map_in: Sequence[ipc.SnapshotFile], map_out: Sequence[ipc.SnapshotFile]) -> bool:
            snapshotfiles = map_in
            progress.download_success((snapshotfiles[0].file_size + 1) * len(snapshotfiles))
            return still_running_callback()

        sorted_all_snapshotfiles = sorted(all_snapshotfiles, key=lambda files: -files[0].file_size)

        if not utils.parallel_map_to(
            fun=self._download_snapshotfiles_from_storage,
            iterable=sorted_all_snapshotfiles,
            result_callback=_cb,
            n=self.parallel,
        ):
            progress.add_fail()
            progress.done()
            return

        # Delete files that were not supposed to exist
        for relative_path in set(self.snapshot.get_all_paths()).difference(valid_relative_path_set):
            absolute_path = self.dst / relative_path
            absolute_path.unlink(missing_ok=True)

        if self.copy_dst_owner:
            # Adjust owner of created files and folders to be like the owner of dst
            dst_owner_uid = self.dst.stat().st_uid
            chowned_paths: set[str] = set()
            for snapshotfile in snapshotstate.files:
                absolute_path = self.dst / snapshotfile.relative_path
                chown_candidate = absolute_path
                while chown_candidate != self.dst and chown_candidate.stat().st_uid != dst_owner_uid:
                    chowned_paths.add(str(chown_candidate.relative_to(self.dst)))
                    chown_candidate = chown_candidate.parent

            chunk_size = 1000
            sorted_paths = sorted(chowned_paths)
            for chunk_start in range(0, len(sorted_paths), chunk_size):
                astacus_user = getpass.getuser()
                dst_owner_gid = self.dst.stat().st_gid
                # We're very specific to allow a sufficiently restrictive sudoers configuration
                cmd: Sequence[str | Path] = [
                    "/usr/bin/sudo",
                    "/usr/bin/chown",
                    f"--from={astacus_user}:{dst_owner_gid}",
                    f"{dst_owner_uid}",
                    "--",
                    *sorted_paths[chunk_start : chunk_start + chunk_size],
                ]
                subprocess.run(cmd, shell=False, check=True, cwd=self.dst)
        # This operation is done. It may or may not have been a success.
        progress.done()


class DownloadOp(NodeOp[ipc.SnapshotDownloadRequest, ipc.NodeResult]):
    snapshotter: Snapshotter | None = None

    @property
    def storage(self) -> RohmuStorage:
        assert self.config.object_storage is not None
        return RohmuStorage(self.config.object_storage, storage=self.req.storage)

    def create_result(self) -> ipc.NodeResult:
        return ipc.NodeResult()

    def start(self, snapshotter: Snapshotter) -> NodeOp.StartResult:
        logger.info("start_download %r", self.req)
        self.snapshotter = snapshotter
        return self.start_op(op_name="download", op=self, fun=self.download)

    def download(self) -> None:
        assert self.snapshotter is not None
        # Actual 'restore from backup'
        snapshot = download_snapshot(self.storage, self.req.backup_name, self.req.snapshot_index)
        snapshotstate = snapshot.state
        assert snapshotstate is not None

        # 'snapshotter' is global; ensure we have sole access to it
        with self.snapshotter.lock:
            self.check_op_id()
            downloader = Downloader(
                dst=self.config.root,
                snapshotter=self.snapshotter,
                storage=self.storage,
                parallel=self.config.parallel.downloads,
                copy_dst_owner=self.config.copy_root_owner,
            )
            downloader.download_from_storage(
                snapshotstate=snapshotstate,
                progress=self.result.progress,
                still_running_callback=self.still_running_callback,
            )


class Skip(msgspec.Struct):
    pass


def download_snapshot(json_storage: JsonStorage, backup_name: str, index: int) -> ipc.SnapshotResult:
    snapshot_results = msgspec.defstruct(
        "SnapshotResults",
        [*[(f"skip_{i}", Skip) for i in range(index)], ("value", ipc.SnapshotResult)],
        array_like=True,
    )
    partial_manifest = msgspec.defstruct("PartialManifest", [("snapshot_results", snapshot_results)])
    manifest = json_storage.download_json(backup_name, partial_manifest)
    return manifest.snapshot_results.value  # type: ignore[attr-defined]
