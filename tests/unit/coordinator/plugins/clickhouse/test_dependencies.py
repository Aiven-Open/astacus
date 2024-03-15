"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from astacus.coordinator.plugins.clickhouse.dependencies import (
    access_entities_sorted_by_dependencies,
    parse_create_materialized_view,
    tables_sorted_by_dependencies,
)
from astacus.coordinator.plugins.clickhouse.manifest import AccessEntity, Table

import pytest
import uuid

pytestmark = [pytest.mark.clickhouse]


def test_tables_sorted_by_dependencies() -> None:
    t1 = Table(
        database=b"db_one",
        name=b"t1",
        engine="DontCare",
        uuid=uuid.UUID(int=0),
        create_query=b"",
        dependencies=[(b"db_two", b"t4")],
    )
    t2 = Table(
        database=b"db_one",
        name=b"t2",
        engine="DontCare",
        uuid=uuid.UUID(int=0),
        create_query=b"",
        dependencies=[],
    )
    t3 = Table(
        database=b"db_one",
        name=b"t3",
        engine="DontCare",
        uuid=uuid.UUID(int=0),
        create_query=b"",
        dependencies=[(b"db_one", b"t2")],
    )
    t4 = Table(
        database=b"db_two",
        name=b"t4",
        engine="DontCare",
        uuid=uuid.UUID(int=0),
        create_query=b"",
        dependencies=[(b"db_one", b"t2"), (b"db_one", b"t3")],
    )
    assert tables_sorted_by_dependencies([t1, t2, t3, t4]) == [t1, t4, t3, t2]
    assert tables_sorted_by_dependencies([t2, t3, t4, t1]) == [t1, t4, t3, t2]


def test_materialized_view_to_table_dependency() -> None:
    t1 = Table(
        database=b"db_one",
        name=b"t1",
        engine="DontCare",
        uuid=uuid.UUID(int=0),
        create_query=b"",
        dependencies=[(b"db_two", b"t4")],
    )
    mv1 = Table(
        database=b"db_one",
        name=b"mv1",
        engine="MaterializedView",
        uuid=uuid.UUID(int=0),
        create_query=b"CREATE MATERIALIZED VIEW db.mv1 TO db_one.t2 AS SELECT 1",
        dependencies=[(b"db_one", b"mv2")],
    )
    mv2 = Table(
        database=b"db_one",
        name=b"mv2",
        engine="MaterializedView",
        uuid=uuid.UUID(int=0),
        create_query=b"CREATE MATERIALIZED VIEW db.mv2 TO db_one.t1 AS SELECT 1",
        dependencies=[],
    )
    assert tables_sorted_by_dependencies([t1, mv1, mv2]) == [t1, mv1, mv2]
    assert tables_sorted_by_dependencies([mv2, mv1, t1]) == [t1, mv1, mv2]


def test_materialized_view_inner_table_dependency() -> None:
    t1 = Table(
        database=b"db_one",
        name=b"t1",
        engine="DontCare",
        uuid=uuid.UUID("abf6400f-4976-4e2f-9296-7b9447b31bc9"),
        create_query=b"",
        dependencies=[(b"db_two", b"t4")],
    )
    mv1 = Table(
        database=b"db_one",
        name=b"mv1",
        engine="MaterializedView",
        uuid=uuid.UUID(int=0),
        create_query=b"CREATE MATERIALIZED VIEW db.mv1 TO INNER UUID 'abf6400f-4976-4e2f-9296-7b9447b31bc9' AS SELECT 1",
        dependencies=[(b"db_one", b"mv2")],
    )
    mv2 = Table(
        database=b"db_one",
        name=b"mv2",
        engine="MaterializedView",
        uuid=uuid.UUID(int=0),
        create_query=b"CREATE MATERIALIZED VIEW db.mv2 TO db_one.t1 AS SELECT 1",
        dependencies=[],
    )
    assert tables_sorted_by_dependencies([t1, mv1, mv2]) == [t1, mv1, mv2]
    assert tables_sorted_by_dependencies([mv2, mv1, t1]) == [t1, mv1, mv2]



@pytest.mark.parametrize(
    "database,table,inner_uuid,query",
    [
        ("default.\" -?2'", "mv_dst1", None, "CREATE MATERIALIZED VIEW db.mv TO `default.\" -?2'`.mv_dst1 AS SELECT 1"),
        ("default.` -?2'", "mv_dst1", None, """CREATE MATERIALIZED VIEW db.mv TO "default.` -?2'".mv_dst1 AS SELECT 1"""),
        ("default", "mv _dst", None, "CREATE MATERIALIZED VIEW db.mv TO default.`mv _dst` AS SELECT 1"),
        ("default", "data_table_for_mv", None, "CREATE MATERIALIZED VIEW default.materialized_view_deleted_source UUID 'f8f42cde-8aed-40ae-a193-fce81c3eac75' TO default.data_table_for_mv (`thekey3` Int32) AS SELECT toInt32(thekey * 3)"),
        (None, None, "abf6400f-4976-4e2f-9296-7b9447b31bc9", "CREATE MATERIALIZED VIEW db.mv UUID '404e94c3-c283-46ef-9705-0676f130f97c' TO INNER UUID 'abf6400f-4976-4e2f-9296-7b9447b31bc9' as select 1"),
    ],
)
def test_get_materialized_view_dependents(database: str, table: str, inner_uuid: str, query: str) -> None:
    assert parse_create_materialized_view(query) == (database, table, inner_uuid)


def test_dangling_table_dependency_doesnt_crash() -> None:
    t1 = Table(
        database=b"db_one",
        name=b"t1",
        engine="DontCare",
        uuid=uuid.UUID(int=0),
        create_query=b"",
        dependencies=[(b"db_two", b"t4")],
    )
    assert tables_sorted_by_dependencies([t1]) == [t1]


def test_access_entities_sorted_by_dependencies() -> None:
    a1 = AccessEntity(
        type="P",
        uuid=uuid.UUID("00000000-abcd-0000-0000-000000000001"),
        name=b"a1",
        attach_query=b"""
            ATTACH ROW POLICY a1 TO ID('00000000-abcd-0000-0000-000000000003'));
        """,
    )
    a2 = AccessEntity(
        type="R",
        uuid=uuid.UUID("00000000-abcd-0000-0000-000000000002"),
        name=b"a2",
        attach_query=b"""
            ATTACH ROLE a2;
            ATTACH GRANT ID('00000000-abcd-0000-0000-000000000004') to a2;
        """,
    )
    a3 = AccessEntity(
        type="U",
        uuid=uuid.UUID("00000000-abcd-0000-0000-000000000003"),
        name=b"a3",
        attach_query=b"""
            ATTACH USER a3;
            ATTACH GRANT ID('00000000-abcd-0000-0000-000000000002') to a3;
            ATTACH GRANT ID('00000000-abcd-0000-0000-000000000004') to a3;
        """,
    )
    a4 = AccessEntity(
        type="R", uuid=uuid.UUID("00000000-0000-abcd-0000-000000000004"), name=b"a4", attach_query=b"ATTACH ROLE a4"
    )
    assert access_entities_sorted_by_dependencies([a1, a2, a3, a4]) == [a4, a2, a3, a1]


def test_dangling_access_entities_doesnt_crash() -> None:
    # ClickHouse takes care of the cohesiveness of the access entity graph, but we can
    # imagine at least two reasons to have a partial graph :
    # - We have references to an entity from another source of users, such as an admin
    #   user hard-coded in users.xml.
    # - A future ClickHouse feature uses IDs that refer to something else than an access
    #   entity, and we accidentally pick up on that id when parsing the attach_query.
    a1 = AccessEntity(
        type="P",
        uuid=uuid.UUID("00000000-abcd-0000-0000-000000000001"),
        name=b"a1",
        attach_query=b"""
            ATTACH ROW POLICY a1 TO ID('00000000-abcd-0000-0000-000000000003'));
        """,
    )
    assert access_entities_sorted_by_dependencies([a1]) == [a1]
