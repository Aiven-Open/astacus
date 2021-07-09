"""

Copyright (c) 2021 Aiven Ltd
See LICENSE for details

Cassandra handling that is run on every node in the Cluster

"""

from .node import NodeOp
from astacus.common import ipc, utils
from astacus.common.cassandra.config import SNAPSHOT_NAME
from astacus.common.cassandra.utils import is_system_keyspace
from typing import List, Optional

import logging
import shutil
import subprocess
import yaml

logger = logging.getLogger(__name__)

SNAPSHOT_GLOB = f"data/*/*/snapshots/{SNAPSHOT_NAME}"


# Clients do not really provide the sub-requests, but it is cleaner to
# have simply single class handling all Cassandra sub-requests
class CassandraRequest(ipc.NodeRequest):
    subop: ipc.CassandraSubOp
    tokens: Optional[List[str]]


class CassandraOp(NodeOp):
    def start(self, *, req):
        subop = req.subop
        if subop == ipc.CassandraSubOp.get_schema_hash:
            self.result = ipc.CassandraGetSchemaHashResult(schema_hash="")
        self.req = req
        logger.debug("start_cassandra %r", req)
        assert self.config.cassandra
        return self.start_op(
            op_name="cassandra",
            op=self,
            fun={
                ipc.CassandraSubOp.start_cassandra: self.start_cassandra,
                ipc.CassandraSubOp.get_schema_hash: self.get_schema_hash,
                ipc.CassandraSubOp.remove_snapshot: self.remove_snapshot,
                ipc.CassandraSubOp.restore_snapshot: self.restore_snapshot,
                ipc.CassandraSubOp.take_snapshot: self.take_snapshot,
            }[subop]
        )

    def get_schema_hash(self):
        """ This is used to get hash of the schema as seen by this node. """
        # pylint: disable=import-outside-toplevel
        from astacus.common.cassandra.client import client_context
        from astacus.common.cassandra.schema import CassandraSchema
        with client_context(self.config.cassandra.client) as cas:
            schema = CassandraSchema.from_cassandra_client(cas)
            self.result.schema_hash = schema.calculate_hash()
        self.result.progress.done()

    def remove_snapshot(self):
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

    def restore_snapshot(self):
        """ This is used to restore the snapshot files into place, with Cassandra offline. """
        progress = self.result.progress
        progress.add_total(3)

        subprocess.call(self.config.cassandra.stop_command)
        progress.add_success()

        # TBD: Delete extra data (current cashew coesn't do it, but we could)

        # Move files from Astacus snapshot directories to the actual data directories
        for table_snapshot in self.config.root.glob(SNAPSHOT_GLOB):
            parts = table_snapshot.parts
            # -2 = snapshots, -1 = name of the snapshots
            table_name_and_id = parts[-3]
            keyspace_name = parts[-4]
            if is_system_keyspace(keyspace_name):
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
            for file_path in table_snapshot.glob("*"):
                # TBD if we should filter something?
                file_path.rename(table_paths[0] / file_path.name)

        subprocess.call(self.config.cassandra.start_command)
        progress.add_success()

        progress.done()

    def start_cassandra(self):
        progress = self.result.progress
        progress.add_total(3)

        with self.config.cassandra.config_path.open() as f:
            config = yaml.safe_load(f)
        progress.add_success()

        config["auto_bootstrap"] = False
        config["initial_token"] = ", ".join(self.req.tokens)
        with utils.open_path_with_atomic_rename(self.config.cassandra.config_path, mode="w") as f:
            yaml.safe_dump(config, f)
        progress.add_success()

        subprocess.call(self.config.cassandra.start_command)
        progress.add_success()

        progress.done()

    def take_snapshot(self):
        """This is used to take snapshot"""
        cmd = self.config.cassandra.nodetool_command[:]
        cmd.extend(["snapshot", "-t", SNAPSHOT_NAME])
        subprocess.call(cmd)
        self.result.progress.done()
