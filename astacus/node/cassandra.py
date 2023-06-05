"""

Copyright (c) 2021 Aiven Ltd
See LICENSE for details

Cassandra handling that is run on every node in the Cluster

"""

from .node import NodeOp
from astacus.common import ipc
from astacus.common.cassandra.client import CassandraClient
from astacus.common.cassandra.config import SNAPSHOT_NAME
from astacus.common.cassandra.utils import is_system_keyspace
from astacus.common.exceptions import TransientException

import contextlib
import logging
import shutil
import subprocess
import tempfile
import yaml

logger = logging.getLogger(__name__)

SNAPSHOT_GLOB = f"data/*/*/snapshots/{SNAPSHOT_NAME}"


class SimpleCassandraSubOp(NodeOp[ipc.NodeRequest, ipc.NodeResult]):
    """
    Generic class to handle no arguments in + no output out case subops.

    Due to that, it does not (really) care about request, and as far
    as result goes it only cares about progress.
    """

    def create_result(self) -> ipc.NodeResult:
        return ipc.NodeResult()

    def start(self, *, req: ipc.NodeRequest, subop: ipc.CassandraSubOp) -> NodeOp.StartResult:
        self.req = req
        assert self.config.cassandra
        return self.start_op(
            op_name="cassandra",
            op=self,
            fun={
                ipc.CassandraSubOp.remove_snapshot: self.remove_snapshot,
                ipc.CassandraSubOp.restore_snapshot: self.restore_snapshot,
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
        progress = self.result.progress
        progress.add_total(1)
        todo = list(self.config.root.glob(SNAPSHOT_GLOB))
        progress.add_success()
        progress.add_total(len(todo))
        for snapshotpath in todo:
            shutil.rmtree(snapshotpath)
            progress.add_success()
        progress.done()

    def restore_snapshot(self) -> None:
        """This is used to restore the snapshot files into place, with Cassandra offline."""
        # TBD: Delete extra data (current cashew doesn't do it, but we could)

        # Move files from Astacus snapshot directories to the actual data directories
        progress = self.result.progress
        table_snapshots = list(self.config.root.glob(SNAPSHOT_GLOB))
        progress.add_total(len(table_snapshots))

        for table_snapshot in table_snapshots:
            parts = table_snapshot.parts
            # -2 = snapshots, -1 = name of the snapshots
            table_name_and_id = parts[-3]
            keyspace_name = parts[-4]
            if is_system_keyspace(keyspace_name):
                progress.add_success()
                continue

            table_name, _ = table_name_and_id.rsplit("-", 1)

            # This could be more efficient too; oh well.
            keyspace_path = table_snapshot.parents[2]
            table_paths = list(keyspace_path.glob(f"{table_name}-*"))
            assert len(table_paths) >= 1, f"NO tables with prefix {table_name}- found in {keyspace_path}!"
            if len(table_paths) > 1:
                # Prefer the one that isn't table_name_and_id
                table_paths = [p for p in table_paths if p.name != table_name_and_id]
            assert len(table_paths) == 1

            table_path = table_paths[0]

            # Ensure destination path is empty except for potential directories (e.g. backups/)
            # This should never have anything - except for system_auth, it gets populated when we restore schema.
            existing_files = [file_path for file_path in table_path.glob("*") if file_path.is_file()]
            if keyspace_name == "system_auth":
                for existing_file in existing_files:
                    existing_file.unlink()
                existing_files = []
            assert not existing_files, f"Files found in {table_name}: {existing_files}"

            for file_path in table_snapshot.glob("*"):
                # TBD if we should filter something?
                file_path.rename(table_path / file_path.name)

            progress.add_success()

        self.result.progress.done()

    def stop_cassandra(self) -> None:
        assert self.config.cassandra
        subprocess.run(self.config.cassandra.stop_command, check=True)
        self.result.progress.done()

    def take_snapshot(self) -> None:
        assert self.config.cassandra
        cmd = self.config.cassandra.nodetool_command[:]
        cmd.extend(["snapshot", "-t", SNAPSHOT_NAME])
        subprocess.run(cmd, check=True)
        self.result.progress.done()


class CassandraStartOp(NodeOp[ipc.CassandraStartRequest, ipc.NodeResult]):
    def create_result(self) -> ipc.NodeResult:
        return ipc.NodeResult()

    def start(self, *, req: ipc.CassandraStartRequest) -> NodeOp.StartResult:
        self.req = req
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

        config["auto_bootstrap"] = False
        if self.req.tokens:
            config["initial_token"] = ", ".join(self.req.tokens)
            config["num_tokens"] = len(self.req.tokens)
        with tempfile.NamedTemporaryFile(mode="w") as config_fh:
            yaml.safe_dump(config, config_fh)
            config_fh.flush()
            progress.add_success()

            subprocess.run(self.config.cassandra.start_command + [config_fh.name], check=True)
            progress.add_success()

        progress.done()


class CassandraGetSchemaHashOp(NodeOp[ipc.NodeRequest, ipc.CassandraGetSchemaHashResult]):
    def start(self, *, req: ipc.NodeRequest) -> NodeOp.StartResult:
        self.req = req
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
