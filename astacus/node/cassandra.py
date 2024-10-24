"""Copyright (c) 2021 Aiven Ltd
See LICENSE for details.

Cassandra handling that is run on every node in the Cluster

"""

from .node import NodeOp
from astacus.common import ipc
from astacus.common.cassandra.client import CassandraClient
from astacus.common.cassandra.config import SNAPSHOT_GLOB, SNAPSHOT_NAME
from astacus.common.exceptions import TransientException
from collections.abc import Callable
from pathlib import Path
from pydantic import DirectoryPath

import contextlib
import logging
import shutil
import subprocess
import tempfile
import yaml

logger = logging.getLogger(__name__)

TABLES_GLOB = "data/*/*"


def ks_table_from_snapshot_path(p: Path) -> tuple[str, str]:
    # /.../keyspace/table/snapshots/astacus
    return p.parts[-4], p.parts[-3]


def ks_table_from_backup_path(p: Path) -> tuple[str, str]:
    # /.../keyspace/table/backups
    return p.parts[-3], p.parts[-2]


class SimpleCassandraSubOp(NodeOp[ipc.NodeRequest, ipc.NodeResult]):
    """Generic class to handle no arguments in + no output out case subops.

    Due to that, it does not (really) care about request, and as far
    as result goes it only cares about progress.
    """

    def create_result(self) -> ipc.NodeResult:
        return ipc.NodeResult()

    def start(self, subop: ipc.CassandraSubOp) -> NodeOp.StartResult:
        assert self.config.cassandra
        return self.start_op(
            op_name="cassandra",
            op=self,
            fun={
                ipc.CassandraSubOp.remove_snapshot: self.remove_snapshot,
                ipc.CassandraSubOp.unrestore_sstables: self.unrestore_sstables,
                ipc.CassandraSubOp.stop_cassandra: self.stop_cassandra,
                ipc.CassandraSubOp.take_snapshot: self.take_snapshot,
            }[subop],
        )

    def remove_snapshot(self) -> None:
        """This is used to remove the current snapshot (if any).

        It is used as prelude for actual Astacus snapshot of the files
        and after the backup has completed.

        Note that Cassandra does not do any internal bookkeeping of
        the snapshots so the rmtrees are enough.
        """
        self._clean_matching(SNAPSHOT_GLOB, clean_func=shutil.rmtree)

    def unrestore_sstables(self) -> None:
        """Remove everything from the data dir except the snapshots and backups.

        This is the "opposite" of restore snapshot subop: it cleans all the live
        data files and leaves only the snapshots and backups directories. Those
        are (or will be) used by Snapshotter as download destinations when restoring.
        If we unlink those, we end up re-downloading the whole snapshot on each
        restore attempt, and that's unnecessary in most cases.
        """
        self._clean_matching(TABLES_GLOB, clean_func=self._remove_sstables)

    def _clean_matching(self, dir_glob: str, *, clean_func: Callable[[Path], None]) -> None:
        progress = self.result.progress
        progress.add_total(1)
        todo = list(self.config.root.glob(dir_glob))
        progress.add_success()
        progress.add_total(len(todo))
        for to_clean in todo:
            clean_func(to_clean)
            progress.add_success()
        progress.done()

    def _remove_sstables(self, table_dir: Path) -> None:
        if not table_dir.is_dir():
            # Doesn't seem worth it to fail the restore attempt because of an existing file
            # that should not have existed anyway.
            logger.warning("Unexpected non-directory path in keyspaces dir: %s", table_dir)
            table_dir.unlink(missing_ok=True)
            return
        if not (table_dir / "snapshots" / SNAPSHOT_NAME).exists():
            # This was created by Cassandra during the schema restore. Remove entirely.
            shutil.rmtree(table_dir)
            return
        for p in table_dir.iterdir():
            # Remove anything except the things controlled by Snapshotter
            if p.name in ("snapshots", "backups"):
                continue
            if p.is_dir():
                shutil.rmtree(p)
            else:
                p.unlink(missing_ok=True)

    def stop_cassandra(self) -> None:
        assert self.config.cassandra
        subprocess.run(self.config.cassandra.stop_command, check=True)
        self.result.progress.done()

    def take_snapshot(self) -> None:
        assert self.config.cassandra
        cmd = list(self.config.cassandra.nodetool_command)
        cmd.extend(["snapshot", "-t", SNAPSHOT_NAME])
        subprocess.run(cmd, check=True)
        self.result.progress.done()


class CassandraRestoreSSTablesOp(NodeOp[ipc.CassandraRestoreSSTablesRequest, ipc.NodeResult]):
    def create_result(self) -> ipc.NodeResult:
        return ipc.NodeResult()

    def start(self) -> NodeOp.StartResult:
        return self.start_op(op_name="cassandra", op=self, fun=self.restore_sstables)

    def restore_sstables(self) -> None:
        """This is used to restore the snapshot files into place, with Cassandra offline."""
        # Move files from Astacus snapshot directories to the actual data directories
        progress = self.result.progress
        table_snapshots = list(self.config.root.glob(self.req.table_glob))
        progress.add_total(len(table_snapshots))

        for table_snapshot in table_snapshots:
            # expected table_snapshot structure: <config.root>/data/ks/tname-tid/...
            keyspace_name, table_name_and_id = table_snapshot.relative_to(self.config.root / "data").parts[:2]
            if keyspace_name in self.req.keyspaces_to_skip:
                progress.add_success()
                continue

            table_path = (
                self.config.root / "data" / keyspace_name / table_name_and_id
                if self.req.match_tables_by == ipc.CassandraTableMatching.cfid
                else self._match_table_by_name(keyspace_name, table_name_and_id)
            )

            if self.req.expect_empty_target:
                self._ensure_target_is_empty(keyspace_name=keyspace_name, table_path=table_path)

            for file_path in table_snapshot.glob("*"):
                file_path.rename(table_path / file_path.name)

            progress.add_success()

        self.result.progress.done()

    def _match_table_by_name(self, keyspace_name: str, table_name_and_id: str) -> DirectoryPath:
        table_name, _ = table_name_and_id.rsplit("-", 1)

        # This could be more efficient too; oh well.
        keyspace_path = self.config.root / "data" / keyspace_name
        table_paths = list(keyspace_path.glob(f"{table_name}-*"))
        if not table_paths:
            raise RuntimeError(f"NO tables with prefix {table_name}- found in {keyspace_path}!")
        if len(table_paths) > 1:
            # Prefer the one that isn't table_name_and_id
            table_paths = [p for p in table_paths if p.name != table_name_and_id]
        if len(table_paths) != 1:
            raise RuntimeError(f"Too many tables with prefix {table_name}- found in {keyspace_path}: {table_paths}")

        return table_paths[0]

    def _ensure_target_is_empty(self, *, keyspace_name: str, table_path: Path) -> None:
        # Ensure destination path is empty except for potential directories (e.g. backups/)
        # This should never have anything - except for system_auth, it gets populated when we restore schema.
        existing_files = [file_path for file_path in table_path.glob("*") if file_path.is_file()]
        if keyspace_name == "system_auth":
            for existing_file in existing_files:
                existing_file.unlink()
            existing_files = []
        if existing_files:
            raise RuntimeError(f"Files found in {table_path.name}: {existing_files}")


class CassandraStartOp(NodeOp[ipc.CassandraStartRequest, ipc.NodeResult]):
    def create_result(self) -> ipc.NodeResult:
        return ipc.NodeResult()

    def start(self) -> NodeOp.StartResult:
        return self.start_op(op_name="cassandra", op=self, fun=self.start_cassandra)

    def start_cassandra(self) -> None:
        assert self.req is not None
        progress = self.result.progress
        progress.add_total(3)

        assert self.config.cassandra
        config_path = self.config.cassandra.client.config_path
        assert config_path

        with config_path.open() as config_read_fh:
            config = yaml.safe_load(config_read_fh)
        progress.add_success()

        config["auto_bootstrap"] = self.req.replace_address_first_boot is not None
        if self.req.tokens:
            config["initial_token"] = ", ".join(self.req.tokens)
            config["num_tokens"] = len(self.req.tokens)
        if self.req.replace_address_first_boot:
            config["replace_address_first_boot"] = self.req.replace_address_first_boot
        if self.req.skip_bootstrap_streaming:
            config["skip_bootstrap_streaming"] = True
        with tempfile.NamedTemporaryFile(mode="w") as config_fh:
            yaml.safe_dump(config, config_fh)
            config_fh.flush()
            progress.add_success()

            subprocess.run([*self.config.cassandra.start_command, config_fh.name], check=True)
            progress.add_success()

        progress.done()


class CassandraGetSchemaHashOp(NodeOp[ipc.NodeRequest, ipc.CassandraGetSchemaHashResult]):
    def start(self) -> NodeOp.StartResult:
        assert self.config.cassandra
        return self.start_op(op_name="cassandra", op=self, fun=self.get_schema_hash)

    def create_result(self) -> ipc.CassandraGetSchemaHashResult:
        return ipc.CassandraGetSchemaHashResult(schema_hash="")

    def _get_schema_hash(self) -> str:
        assert self.config.cassandra
        with CassandraClient(self.config.cassandra.client).connect() as cas:
            rows = cas.execute("SELECT schema_version FROM system.local")
            return rows[0][0]

    def get_schema_hash(self) -> None:
        """This is used to get hash of the schema as seen by this node."""
        with contextlib.suppress(TransientException):
            self.result.schema_hash = self._get_schema_hash()
        self.result.progress.done()
