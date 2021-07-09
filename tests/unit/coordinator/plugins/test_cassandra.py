"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Test that the plugin cassandra specific flow (backup + restore) works

There is some replication in the logic between this and m3db tests,
but the number of shared code lines is pretty small so it does not
seem *really* worth pursuing some shared base.

"""

from ..conftest import COORDINATOR_NODES
from astacus.common import ipc
from astacus.coordinator.config import CoordinatorConfig
from astacus.coordinator.plugins import base
from astacus.coordinator.state import CoordinatorState
from dataclasses import dataclass
from enum import auto, Enum
from urllib.parse import urlsplit

import asyncio
import logging
import pytest
import respx
import uuid

try:
    # pylint: disable=ungrouped-imports
    from astacus.common.cassandra import schema
    from astacus.coordinator.plugins import cassandra
except ImportError:
    pytestmark = pytest.mark.skip

    class cassandra:
        class CassandraBackupOp:
            pass

        class CassandraRestoreOp:
            steps = []


logger = logging.getLogger(__name__)

COORDINATOR_CONFIG = {
    "plugin": "cassandra",
    "poll": {
        "delay_start": 1,
        "duration": 0.1,
        # delay_start > duration == only single attempt
    },
    "plugin_config": {
        "client": {
            "hostnames": ["127.0.0.1"],
            "port": 42,
            "username": "aiven",
            "password": "x",
        },
        "nodes": [{}, {}],
        "restore_start_timeout": 0.1,
    },
}

PLUGIN_DATA = {
    "cassandra_schema": {
        "keyspaces": []
    },
    "nodes": [
        {
            "address": "",
            "host_id": "123e4567-e89b-12d3-a456-426614174000",
            "listen_address": "::1",
            "rack": "aiven",
            "tokens": ["foo", "bar"],
        },
        {
            "address": "",
            "host_id": "123e4567-e89b-12d3-a456-426614174001",
            "listen_address": "::1",
            "rack": "aiven",
            "tokens": ["7", "42"],
        },
    ],
}


class DummyCassandraBackupOp(cassandra.CassandraBackupOp):
    nodes = COORDINATOR_NODES
    request_url = urlsplit("http://127.0.0.1:42/dummyrequest-url")

    def __init__(self):
        # pylint: disable=super-init-not-called
        self.config = CoordinatorConfig.parse_obj(COORDINATOR_CONFIG)
        self.steps = [step for step in self.steps if getattr(base.BackupOpBase, f"step_{step}", None) is None]
        self.state = CoordinatorState()

        # Bit unfortunate but that's life; should probably refactor
        # the base class so that some of the init functionality is not
        # in the __init__ which depends on Coordinator object
        self.subresult_received_event = asyncio.Event()

    async def step_retrieve_manifest(self):
        # We don't want to really deal with Cassandra here; however,
        # create some fictional data just to show that the schema
        # validation for this input is happy.
        keyspace = schema.CassandraKeyspace(
            name="k",
            cql_create_self="self",
            cql_create_all="all",
            aggregates=[],
            functions=[],
            tables=[],
            user_types=[],
        )
        nodes = [
            cassandra.CassandraManifestNode(
                address="127.0.0.1", host_id=uuid.uuid1(), listen_address="::1", rack="az-a", tokens=["42"]
            )
        ]
        manifest = cassandra.CassandraManifest(
            cassandra_schema=schema.CassandraSchema(keyspaces=[keyspace]),
            nodes=nodes,
        )
        return manifest


class BackupFail(Enum):
    none = auto()

    shutting_down = auto()

    get_schema_hash = auto()
    result_get_schema_hash = auto()

    remove_snapshot = auto()
    result_remove_snapshot = auto()

    take_snapshot = auto()
    result_take_snapshot = auto()


def mock_cassandra_subops(*, fail_at, fail_enum):
    subop_result_extra_content = {
        ipc.CassandraSubOp.get_schema_hash: {
            "schema_hash": "hash"
        },
    }
    for port in [12345, 12346]:
        baseurl = f"http://localhost:{port}/asdf/cassandra"
        for subop in ipc.CassandraSubOp:
            fail_at_name = getattr(fail_enum, subop.name, None)
            if fail_at_name is None:
                logger.info("Subop %r not covered by %r", subop, fail_enum)
                continue
            status_url = f"{baseurl}/result-{subop.value}"
            respx.post(
                f"{baseurl}/{subop.value}",
                content={
                    "op_id": 42,
                    "status_url": status_url
                },
                status_code=200 if fail_at != fail_at_name else 500,
            )
            content = {"progress": {"final": True}}
            content.update(subop_result_extra_content.get(subop, {}))
            fail_at_result_name = getattr(fail_enum, f"result_{subop.name}", None)
            if not fail_at_result_name:
                logger.info("Subop %r result not covered by %r", subop, fail_enum)
                continue
            respx.get(
                status_url,
                content=content,
                status_code=200 if fail_at != fail_at_result_name else 500,
            )


@pytest.mark.timeout(1)
@pytest.mark.asyncio
@pytest.mark.parametrize("fail_at", list(BackupFail))
async def test_cassandra_backup(fail_at: BackupFail):
    op = DummyCassandraBackupOp()
    assert op.steps == [
        'init',
        'retrieve_schema_hash',
        'retrieve_manifest',
        'remove_cassandra_snapshot',
        'take_cassandra_snapshot',
        'retrieve_schema_hash_again',
        'remove_cassandra_snapshot_again',
    ]
    with respx.mock:
        op.state.shutting_down = fail_at == BackupFail.shutting_down
        mock_cassandra_subops(fail_at=fail_at, fail_enum=BackupFail)
        assert await op.try_run() == (fail_at == BackupFail.none)


class DummyCassandraRestoreOp(cassandra.CassandraRestoreOp):
    nodes = COORDINATOR_NODES

    request_url = urlsplit("http://127.0.0.1:42/dummyrequest-url")

    # skip some steps we don't particularly want (or need) to test:
    #
    # - backup_name: we provide explicit backup manifest as we don't
    #   want to deal with object storage
    # - restore: we don't want to actually deal with object storage etc.
    steps = [step for step in cassandra.CassandraRestoreOp.steps if step not in {
        "backup_name",
        "restore",
    }]

    def __init__(self):
        # pylint: disable=super-init-not-called
        self.config = CoordinatorConfig.parse_obj(COORDINATOR_CONFIG)
        self.state = CoordinatorState()
        req = ipc.RestoreRequest()
        self.req = req

        # Bit unfortunate but that's life; should probably refactor
        # the base class so that some of the init functionality is not
        # in the __init__ which depends on Coordinator object
        self.subresult_received_event = asyncio.Event()

    result_backup_name = "x"

    async def download_backup_manifest(self, backup_name):
        assert backup_name == self.result_backup_name
        return ipc.BackupManifest.parse_obj({
            "plugin": "cassandra",
            "plugin_data": PLUGIN_DATA,
            "attempt": 1,
            "snapshot_results": [{}, {}],
            "start": "2020-01-01 12:00",
            "upload_results": [],
        })


class RestoreFail(Enum):
    none = auto()

    shutting_down = auto()

    get_schema_hash = auto()
    result_get_schema_hash = auto()

    start_cassandra = auto()
    result_start_cassandra = auto()

    restore_snapshot = auto()
    result_restore_snapshot = auto()


@dataclass
class RestoreTest:
    fail_at: RestoreFail = RestoreFail.none


@pytest.mark.timeout(1)
@pytest.mark.asyncio
@pytest.mark.parametrize("rt", [RestoreTest(fail_at=fail_at) for fail_at in RestoreFail])
async def test_cassandra_restore(rt: RestoreTest, mocker):

    # We don't really want to run the sync code which deals with Cassandra
    mocker.patch.object(cassandra, "run_in_threadpool")

    fail_at = rt.fail_at
    op = DummyCassandraRestoreOp()
    assert op.steps == [
        'init',
        'backup_manifest',
        'start_cassandra',
        'wait_cassandra_up_again',
        'retrieve_schema_hash',
        'restore_schema_pre_data',
        'restore_cassandra_snapshot',
        'wait_cassandra_up_again',
        'restore_schema_post_data',
    ]
    with respx.mock:
        op.state.shutting_down = fail_at == RestoreFail.shutting_down
        mock_cassandra_subops(fail_at=fail_at, fail_enum=RestoreFail)
        assert await op.try_run() == (fail_at is RestoreFail.none)
