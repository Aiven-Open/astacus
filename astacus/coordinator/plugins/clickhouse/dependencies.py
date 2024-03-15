"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details
"""
from astacus.coordinator.plugins.clickhouse.manifest import AccessEntity, Table
from collections.abc import Sequence

# noinspection PyCompatibility
import graphlib
import re
import uuid

def sort_topologically(elements: Sequence, get_key: callable,
                       get_dependencies: callable=lambda x: [],
                       get_dependants: callable=lambda x: []) -> list:
    """
    Sort elements topologically based on their dependencies.
    """
    sorter = graphlib.TopologicalSorter()
    for element in elements:
        element_key = get_key(element)
        sorter.add(element_key)
        for dependency in get_dependencies(element):
            sorter.add(element_key, dependency)
        for dependency in get_dependants(element):
            sorter.add(dependency, element_key)
    sort_order = list(sorter.static_order())
    return sorted(elements, key=lambda element: sort_order.index(get_key(element)))

def undelimit(name: str | None) -> str | None:
    if name is None:
        return None
    if name[0] == "`" or name[0] == '"':
        return name[1:-1]
    return name

def parse_create_materialized_view(query):
    # TODO: check if extra escape is needed like escape_sql_identifier
    delimiter_regexp = r"""(`[^`]*`|[a-zA-Z0-9_]+|"[^"]*")"""
    uuid_regexp = r"'([a-fA-F0-9\-]+)'"
    match = re.match(r"CREATE MATERIALIZED VIEW\s+"
                     rf"{delimiter_regexp}\.{delimiter_regexp}\s+"
                     rf"(?:UUID\s+{uuid_regexp}\s+)?"
                     rf"(?:TO\s+{delimiter_regexp}\.{delimiter_regexp}|TO\s+INNER\s+UUID\s+{uuid_regexp})\s+"
                     , query)
    if match is None:
        return None, None, None
    db_name = undelimit(match.group(4))
    table_name = undelimit(match.group(5))
    inner_uuid = match.group(6)
    return db_name, table_name, inner_uuid

def tables_sorted_by_dependencies(tables: Sequence[Table]) -> Sequence[Table]:
    """
    Takes a list of `(database_name: str, table: Table)` and returns a new list,
    sorted in a valid order to create each table sequentially.

    This order is required because tables can depend on each other, for example
    tables using the Distributed table engine depends on their source tables.

    The `dependencies` attribute of each table must contain the list of
    `(database_name: str, table_name: str)` that depend on this table.
    """
    uuid_to_dable = {table.uuid: table for table in tables}
    def get_mv_to_table_dependency(table):
        materialized_view_engine = "MaterializedView"
        if table.engine != materialized_view_engine:
            return []
        # TO table or implicit TO INNER should be used
        db_name, table_name, inner_uuid = parse_create_materialized_view(table.create_query.decode("utf-8"))
        if db_name is not None and table_name is not None:
            return [(db_name, table_name)]
        assert inner_uuid is not None
        inner_table = uuid_to_dable.get(uuid.UUID(inner_uuid))
        return [(inner_table.database, inner_table.name)] if inner_table is not None else []

    return sort_topologically(tables,
                              get_key=lambda table: (table.database, table.name),
                              get_dependants=lambda table: table.dependencies,
                              get_dependencies=get_mv_to_table_dependency)


def access_entities_sorted_by_dependencies(access_entities: Sequence[AccessEntity]) -> Sequence[AccessEntity]:
    """
    Takes a list of `AccessEntity` and returns a new list, sorted in a valid order to
    create each entity sequentially.

    This order is required because entities can depend on each other. Most of them could
    easily be sorted by using only the entity type (roles depends on users, so all user
    should be created before all roles). However, some entity types are more complicated:
    roles can depend on other roles. This forces us to use a real topological sort to
    determine the creation order.
    """
    sorter = graphlib.TopologicalSorter()  # type: ignore
    # Unlike tables, ClickHouse does not provide a list of dependencies between entities.
    # This means we need to parse the `attach_query` of the entity to find the uuid of
    # other entities. This is unpleasant, but the quoting format of entity names and entity
    # uuids is different enough to not risk false matches.
    clickhouse_id = re.compile(rb"ID\('([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})'\)")
    return sort_topologically(
        access_entities,
        get_dependencies=lambda entity: map(lambda uuid_bytes: uuid.UUID(uuid_bytes.decode()),
                                       clickhouse_id.findall(entity.attach_query)),
        get_key=lambda entity: entity.uuid)
