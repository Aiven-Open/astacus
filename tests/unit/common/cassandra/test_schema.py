"""
Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""

import pytest

# pylint: disable=protected-access


@pytest.fixture(name="schema", scope="session")
def fixture_schema():
    return pytest.importorskip("astacus.common.cassandra.schema", reason="Cassandra driver is not available")


def test_schema(schema, mocker):

    cut = schema.CassandraUserType(name="cut", cql_create_self="CREATE-USER-TYPE", field_types=["type1", "type2"])
    cfunction = schema.CassandraFunction(name="cf", cql_create_self="CREATE-FUNCTION", argument_types=["atype1", "atype2"])

    cagg = schema.CassandraAggregate(
        name="cagg",
        cql_create_self="CREATE-AGGREGATE",
        argument_types=["aggtype1", "aggtype2"],
    )

    cindex = schema.CassandraIndex(name="cindex", cql_create_self="CREATE-INDEX")

    ctrigger = schema.CassandraTrigger(name="ctrigger", cql_create_self="CREATE-TRIGGER")

    cmv = schema.CassandraIndex(name="cmv", cql_create_self="CREATE-MATERIALIZED-VIEW")

    ctable = schema.CassandraTable(
        name="ctable", cql_create_self="CREATE-TABLE", indexes=[cindex], materialized_views=[cmv], triggers=[ctrigger]
    )

    cks = schema.CassandraKeyspace(
        name="cks",
        cql_create_all="CREATE-KEYSPACE-ALL",
        cql_create_self="CREATE-KEYSPACE",
        aggregates=[cagg],
        functions=[cfunction],
        tables=[ctable],
        user_types=[cut],
    )

    cks_system = schema.CassandraKeyspace(
        name="system",
        cql_create_all="CREATE-SYSTEM-KEYSPACE-ALL",
        cql_create_self="CREATE-SYSTEM-KEYSPACE",
        aggregates=[],
        functions=[],
        tables=[],
        user_types=[],
    )

    cs = schema.CassandraSchema(keyspaces=[cks, cks_system])

    # If the content above changes - or something in the schema hash
    # calculation side changes, this hash needs to be updated:
    assert cs.calculate_hash() == "ec6042f45aedddb79fbe6245c32f2c4fcbf0b0674242a1bbf15d5a9a2b9c26bc"

    # TBD: Better verification of results.
    # For now we just exercise codepaths but with magicmock target they may supply almost anything
    cas = mocker.MagicMock()
    cs.restore_pre_data(cas)
    cs.restore_post_data(cas)


def test_schema_keyspace_iterate_user_types_in_restore_order(schema):
    ut1 = schema.CassandraUserType(name="ut1", cql_create_self="", field_types=[])
    ut2 = schema.CassandraUserType(name="ut2", cql_create_self="", field_types=["ut3", "map<str,frozen<ut1>>"])
    ut3 = schema.CassandraUserType(name="ut3", cql_create_self="", field_types=["ut4"])
    ut4 = schema.CassandraUserType(name="ut4", cql_create_self="", field_types=["something"])
    ks = schema.CassandraKeyspace(
        name="unused",
        cql_create_all="unused",
        cql_create_self="unused",
        aggregates=[],
        functions=[],
        tables=[],
        user_types=[ut1, ut2, ut3, ut4],
    )
    uts = list(ks.iterate_user_types_in_restore_order())
    expected_uts = [ut1, ut4, ut3, ut2]
    assert uts == expected_uts

    # Ensure loop detection works
    ut5 = schema.CassandraUserType(name="ut5", cql_create_self="", field_types=["ut5"])
    ks.user_types = [ut5]
    with pytest.raises(ValueError):
        uts = list(ks.iterate_user_types_in_restore_order())
        print(uts)  # unreachable unless there is a bug


@pytest.mark.parametrize(
    "definition,identifiers",
    [
        ("foo", ["foo"]),
        ("map<frozen<foo>>", ["foo"]),
        ('"q""u""o""t""e""d"', ['q"u"o"t"e"d']),
    ],
)
def test_iterate_identifiers_in_cql_type_definition(schema, definition, identifiers):
    got_identifiers = list(schema._iterate_identifiers_in_cql_type_definition(definition))
    assert got_identifiers == identifiers
