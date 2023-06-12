"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from astacus.coordinator.plugins.clickhouse.manifest import (
    AccessEntity,
    ClickHouseBackupVersion,
    ClickHouseManifest,
    ReplicatedDatabase,
    Table,
)
from base64 import b64encode

import pytest
import uuid

pytestmark = [pytest.mark.clickhouse]

SAMPLE_ACCESS_ENTITY = AccessEntity(type="U", uuid=uuid.UUID(int=0), name=b"bad\x80user", attach_query=b"ATTACH USER ...")
SERIALIZED_ACCESS_ENTITY = {
    "type": "U",
    "uuid": "00000000-0000-0000-0000-000000000000",
    "name": b64encode(b"bad\x80user").decode(),
    "attach_query": b64encode(b"ATTACH USER ...").decode(),
}
SAMPLE_DATABASE = ReplicatedDatabase(name=b"bad\x80db", uuid=uuid.UUID(int=1), shard=b"{my_shard}", replica=b"{my_replica}")
SERIALIZED_DATABASE = {
    "name": b64encode(b"bad\x80db").decode(),
    "uuid": "00000000-0000-0000-0000-000000000001",
    "shard": b64encode(b"{my_shard}").decode(),
    "replica": b64encode(b"{my_replica}").decode(),
}
SAMPLE_TABLE = Table(
    database=b"d\x80b",
    name=b"tab\x80e",
    uuid=uuid.UUID(int=0),
    engine="ReplicatedMergeTree",
    create_query=b"CREATE TABLE ...",
    dependencies=[(b"db", b"othertable")],
)
SERIALIZED_TABLE = {
    "database": b64encode(b"d\x80b").decode(),
    "name": b64encode(b"tab\x80e").decode(),
    "uuid": "00000000-0000-0000-0000-000000000000",
    "engine": "ReplicatedMergeTree",
    "create_query": b64encode(b"CREATE TABLE ...").decode(),
    "dependencies": [[b64encode(b"db").decode(), b64encode(b"othertable").decode()]],
}


@pytest.mark.parametrize(
    "engine,is_replicated,requires_freezing",
    [
        ("AggregatingMergeTree", False, True),
        ("Buffer", False, False),
        ("CollapsingMergeTree", False, True),
        ("COSN", False, False),
        ("Dictionary", False, False),
        ("Distributed", False, False),
        ("EmbeddedRocksDB", False, False),
        ("ExternalDistributed", False, False),
        ("File", False, False),
        ("GenerateRandom", False, False),
        ("GraphiteMergeTree", False, True),
        ("HDFS", False, False),
        ("JDBC", False, False),
        ("Join", False, False),
        ("Kafka", False, False),
        ("LiveView", False, False),
        ("Log", False, False),
        ("MaterializedView", False, False),
        ("Memory", False, False),
        ("Merge", False, False),
        ("MergeTree", False, True),
        ("MongoDB", False, False),
        ("MySQL", False, False),
        ("Null", False, False),
        ("ODBC", False, False),
        ("PostgreSQL", False, False),
        ("RabbitMQ", False, False),
        ("ReplacingMergeTree", False, True),
        ("ReplicatedAggregatingMergeTree", True, True),
        ("ReplicatedCollapsingMergeTree", True, True),
        ("ReplicatedGraphiteMergeTree", True, True),
        ("ReplicatedMergeTree", True, True),
        ("ReplicatedReplacingMergeTree", True, True),
        ("ReplicatedSummingMergeTree", True, True),
        ("ReplicatedVersionedCollapsingMergeTree", True, True),
        ("S3", False, False),
        ("Set", False, False),
        ("StripeLog", False, False),
        ("SummingMergeTree", False, True),
        ("TinyLog", False, False),
        ("URL", False, False),
        ("VersionedCollapsingMergeTree", False, True),
        ("View", False, False),
    ],
)
def test_clickhouse_table_attributes(engine: str, is_replicated: bool, requires_freezing: bool) -> None:
    table = Table(database=b"db", name=b"name", uuid=uuid.UUID(int=0), engine=engine, create_query=b"")
    assert table.is_replicated is is_replicated
    assert table.requires_freezing is requires_freezing


def test_table_escaped_identifier() -> None:
    table = Table(
        database="débé".encode(), name="na`me".encode(), uuid=uuid.UUID(int=0), engine="DontCare", create_query=b""
    )
    assert table.escaped_sql_identifier == "`d\\xc3\\xa9b\\xc3\\xa9`.`na\\`me`"


def test_access_entity_from_plugin_data() -> None:
    assert AccessEntity.from_plugin_data(SERIALIZED_ACCESS_ENTITY) == SAMPLE_ACCESS_ENTITY


def test_replicated_database_from_plugin_data() -> None:
    assert ReplicatedDatabase.from_plugin_data(SERIALIZED_DATABASE) == SAMPLE_DATABASE


def test_table_from_plugin_data() -> None:
    assert Table.from_plugin_data(SERIALIZED_TABLE) == SAMPLE_TABLE


def test_clickhouse_manifest_from_plugin_data() -> None:
    manifest = ClickHouseManifest.from_plugin_data(
        {
            "version": "v2",
            "access_entities": [SERIALIZED_ACCESS_ENTITY],
            "replicated_databases": [SERIALIZED_DATABASE],
            "tables": [SERIALIZED_TABLE],
        }
    )
    assert manifest == ClickHouseManifest(
        version=ClickHouseBackupVersion.V2,
        access_entities=[SAMPLE_ACCESS_ENTITY],
        replicated_databases=[SAMPLE_DATABASE],
        tables=[SAMPLE_TABLE],
    )


def test_clickhouse_manifest_from_v1_plugin_data() -> None:
    manifest = ClickHouseManifest.from_plugin_data(
        {
            "access_entities": [SERIALIZED_ACCESS_ENTITY],
            "replicated_databases": [SERIALIZED_DATABASE],
            "tables": [SERIALIZED_TABLE],
        }
    )
    assert manifest == ClickHouseManifest(
        version=ClickHouseBackupVersion.V1,
        access_entities=[SAMPLE_ACCESS_ENTITY],
        replicated_databases=[SAMPLE_DATABASE],
        tables=[SAMPLE_TABLE],
    )


def test_clickhouse_manifest_to_plugin_data() -> None:
    serialized_manifest = ClickHouseManifest(
        version=ClickHouseBackupVersion.V2,
        access_entities=[SAMPLE_ACCESS_ENTITY],
        replicated_databases=[SAMPLE_DATABASE],
        tables=[SAMPLE_TABLE],
    ).to_plugin_data()
    assert serialized_manifest == {
        "version": "v2",
        "access_entities": [SERIALIZED_ACCESS_ENTITY],
        "replicated_databases": [SERIALIZED_DATABASE],
        "tables": [SERIALIZED_TABLE],
        "object_storage_files": [],
    }
