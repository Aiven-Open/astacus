"""
Copyright (c) 2021 Aiven Ltd
See LICENSE for details

Schema-related parts are based on basebackup_schema.py of Cashew

"""

from .client import CassandraSession
from .utils import is_system_keyspace
from astacus.common.utils import AstacusModel
from cassandra import metadata as cm
from typing import Any, Iterator, List, Set

import hashlib
import itertools
import logging
import re

logger = logging.getLogger(__name__)

# https://cassandra.apache.org/doc/trunk/cassandra/cql/definitions.html
UNQUOTED_IDENTIFIER_RE = "[a-zA-Z][a-zA-Z0-9_]*"
QUOTED_IDENTIFIER_RE = '"(?:[^"]|"")+"'
IDENTIFIER_RE = f"(?:{UNQUOTED_IDENTIFIER_RE}|{QUOTED_IDENTIFIER_RE})"


class CassandraNamed(AstacusModel):
    """~abstract baseclass for something that has a name, and has cql for creating itself."""

    name: str
    cql_create_self: str

    def __lt__(self, o: "CassandraNamed") -> bool:
        # Almost all object names are unique in Cassandra, with
        # exception of functions (see below)
        return self.name < o.name

    @classmethod
    def from_cassandra_metadata(cls, metadata: Any) -> Any:
        return cls(name=metadata.name, cql_create_self=metadata.as_cql_query())

    def restore_if_needed(self, cas: CassandraSession, existing_map: dict) -> None:
        if self.name in existing_map:
            logger.debug("%r with name %r already exists", self.__class__.__name__, self.name)
            return
        logger.info("Creating %r %r", self.__class__.__name__, self.name)
        cas.execute_on_all(self.cql_create_self)


def _iterate_identifiers_in_cql_type_definition(definition: str) -> Iterator[str]:
    # parsing might be more proper, but this is sufficient
    for identifier in re.findall(IDENTIFIER_RE, definition):
        if identifier.startswith('"'):
            # quoted identifier
            unquoted = identifier[1:-1].replace('""', '"')
            yield unquoted
        else:
            # this lower is spurious, but here for
            # understandability - Cassandra normalizes
            # unquoted identifiers by default
            normalized = identifier.lower()

            # these are picked from https://cassandra.apache.org/doc/trunk/cassandra/cql/types.html
            if normalized not in {"map", "set", "list", "frozen", "tuple"}:
                yield normalized


class CassandraUserType(CassandraNamed):
    field_types: List[str]

    @classmethod
    def from_cassandra_metadata(cls, metadata: cm.UserType) -> "CassandraUserType":
        return cls(name=metadata.name, cql_create_self=metadata.as_cql_query(), field_types=metadata.field_types)

    def referred_normalized_identifiers(self) -> Set[str]:
        def iterate_identifiers() -> Iterator[str]:
            for field_type in self.field_types:
                yield from _iterate_identifiers_in_cql_type_definition(field_type)

        return set(iterate_identifiers())

    def restore_in_keyspace(self, cas: CassandraSession, keyspace_metadata: cm.KeyspaceMetadata) -> None:
        self.restore_if_needed(cas, keyspace_metadata.user_types)


class CassandraFunction(CassandraNamed):
    argument_types: List[str]

    def __lt__(self, o: CassandraNamed) -> bool:
        assert isinstance(o, CassandraFunction)
        # Cassandra can have multiple functions with same name, as
        # long as argument types differ. So use them for sorting too.
        if self.name < o.name:
            return True
        if self.argument_types < o.argument_types:
            return True
        return False

    @classmethod
    def get_create_statement(cls, metadata: cm.Function) -> str:
        """
        Based on Function.as_cql_query, but doesn't strip `frozen` from arguments as they need to be specified on restoration
        With default cassandra-driver the function fails to create with an error message like "Non-frozen UDTs are not
        allowed inside collections: ..."
        """
        sep = " "
        keyspace = cm.protect_name(metadata.keyspace)
        name = cm.protect_name(metadata.name)
        arg_list = ", ".join(
            "{} {}".format(cm.protect_name(arg_name), arg_type)
            for arg_name, arg_type in zip(metadata.argument_names, metadata.argument_types)
        )
        typ = metadata.return_type
        lang = metadata.language
        body = metadata.body
        on_null = "CALLED" if metadata.called_on_null_input else "RETURNS NULL"

        return (
            f"CREATE FUNCTION {keyspace}.{name}({arg_list}){sep}"
            f"{on_null} ON NULL INPUT{sep}"
            f"RETURNS {typ}{sep}"
            f"LANGUAGE {lang}{sep}"
            f"AS $${body}$$"
        )

    @classmethod
    def from_cassandra_metadata(cls, metadata: cm.Function) -> "CassandraFunction":
        return cls(
            name=metadata.name,
            cql_create_self=cls.get_create_statement(metadata=metadata),
            argument_types=metadata.argument_types,
        )

    def get_metadata_dict(self, keyspace_metadata: cm.KeyspaceMetadata) -> dict:
        return keyspace_metadata.functions

    def restore_in_keyspace(self, cas: CassandraSession, keyspace_metadata: cm.KeyspaceMetadata) -> None:
        pretty_name = "{}.{}({})".format(
            keyspace_metadata.name,
            self.name,
            ", ".join(self.argument_types),
        )
        for funcagg_metadata in self.get_metadata_dict(keyspace_metadata).values():
            if funcagg_metadata.name == self.name and funcagg_metadata.argument_types == self.argument_types:
                logger.info("%s %s already exists, not creating it", self.__class__.__name__, pretty_name)
                break
        else:
            logger.info("Creating %s %s", self.__class__.__name__, pretty_name)
            cas.execute_on_all(self.cql_create_self)


class CassandraAggregate(CassandraFunction):
    @classmethod
    def get_create_statement(cls, metadata: cm.Aggregate) -> str:
        sep = " "
        keyspace = cm.protect_name(metadata.keyspace)
        name = cm.protect_name(metadata.name)
        type_list = ", ".join(metadata.argument_types)
        state_func = cm.protect_name(metadata.state_func)
        state_type = metadata.state_type

        ret = f"CREATE AGGREGATE {keyspace}.{name}({type_list}){sep}" f"SFUNC {state_func}{sep}" f"STYPE {state_type}"

        if metadata.final_func:
            ret += "{}FINALFUNC {}".format(sep, cm.protect_name(metadata.final_func))
        if metadata.initial_condition:
            ret += f"{sep}INITCOND {metadata.initial_condition}"

        return ret

    @classmethod
    def from_cassandra_metadata(cls, metadata: cm.Aggregate) -> "CassandraAggregate":
        return cls(
            name=metadata.name,
            cql_create_self=cls.get_create_statement(metadata=metadata),
            argument_types=metadata.argument_types,
        )

    def get_metadata_dict(self, keyspace_metadata: cm.KeyspaceMetadata) -> dict:
        return keyspace_metadata.aggregates


class CassandraIndex(CassandraNamed):
    # from_cassandra_metadata covered by superclass
    pass


class CassandraMaterializedView(CassandraNamed):
    # from_cassandra_metadata covered by superclass
    pass


class CassandraTrigger(CassandraNamed):
    # from_cassandra_metadata covered by superclass
    pass


class CassandraTable(CassandraNamed):
    indexes: List[CassandraIndex]
    materialized_views: List[CassandraMaterializedView]
    triggers: List[CassandraTrigger]

    @classmethod
    def from_cassandra_metadata(cls, metadata: cm.TableMetadata) -> "CassandraTable":
        return cls(
            # CassandraNamed
            name=metadata.name,
            cql_create_self=metadata.as_cql_query(),
            # CassandraTable
            indexes=sorted(CassandraIndex.from_cassandra_metadata(metadata) for metadata in metadata.indexes.values()),
            materialized_views=sorted(
                CassandraMaterializedView.from_cassandra_metadata(metadata) for metadata in metadata.views.values()
            ),
            triggers=sorted(CassandraTrigger.from_cassandra_metadata(metadata) for metadata in metadata.triggers.values()),
        )

    def restore_post_data_in_keyspace(self, cas: CassandraSession, keyspace_metadata: cm.KeyspaceMetadata) -> None:
        table_metadata = keyspace_metadata.tables[self.name]
        for resource_map, resource_list in [
            [table_metadata.indexes, self.indexes],
            [table_metadata.triggers, self.triggers],
            [table_metadata.views, self.materialized_views],
        ]:
            for resource in resource_list:
                resource.restore_if_needed(cas, resource_map)


class CassandraKeyspace(CassandraNamed):
    # CQL to create everything
    cql_create_all: str

    aggregates: List[CassandraAggregate]
    functions: List[CassandraFunction]
    tables: List[CassandraTable]
    user_types: List[CassandraUserType]

    @classmethod
    def from_cassandra_metadata(cls, metadata: cm.KeyspaceMetadata) -> "CassandraKeyspace":
        return cls(
            # CassandraNamed
            name=metadata.name,
            cql_create_self=metadata.as_cql_query(),
            # CassandraKeyspace
            cql_create_all=metadata.export_as_string(),
            aggregates=sorted(
                CassandraAggregate.from_cassandra_metadata(metadata) for metadata in metadata.aggregates.values()
            ),
            functions=sorted(
                CassandraFunction.from_cassandra_metadata(metadata) for metadata in metadata.functions.values()
            ),
            tables=sorted(CassandraTable.from_cassandra_metadata(metadata) for metadata in metadata.tables.values()),
            user_types=sorted(
                CassandraUserType.from_cassandra_metadata(metadata) for metadata in metadata.user_types.values()
            ),
        )

    def iterate_user_types_in_restore_order(self) -> Iterator[CassandraUserType]:
        pending = {x.name: (x, x.referred_normalized_identifiers()) for x in self.user_types}
        pending_set = set(pending.keys())
        while pending:
            changed = False
            for user_type_name, (user_type, referred_normalized_identifiers) in list(pending.items()):
                if pending_set & referred_normalized_identifiers:
                    continue
                yield user_type
                del pending[user_type_name]
                changed = True
            if not changed:
                raise ValueError(f"Loop detected in user types: {pending!r}")
            pending_set = set(pending.keys())

    def restore(self, cas: CassandraSession) -> None:
        metadata = cas.cluster_metadata
        self.restore_if_needed(cas, metadata.keyspaces)
        keyspace_metadata: cm.KeyspaceMetadata = metadata.keyspaces[self.name]
        for user_type in self.iterate_user_types_in_restore_order():
            user_type.restore_in_keyspace(cas, keyspace_metadata)
        for table in self.tables:
            table.restore_if_needed(cas, keyspace_metadata.tables)
        # Functions, aggregates are intentionally handled later

    def restore_post_data(self, cas: CassandraSession) -> None:
        metadata = cas.cluster_metadata
        keyspace_metadata: cm.KeyspaceMetadata = metadata.keyspaces[self.name]
        for resource in itertools.chain(self.functions, self.aggregates):
            resource.restore_in_keyspace(cas, keyspace_metadata)

        for table in self.tables:
            table.restore_post_data_in_keyspace(cas, keyspace_metadata)


class CassandraSchema(AstacusModel):
    keyspaces: List[CassandraKeyspace]

    @classmethod
    def from_cassandra_session(cls, cas: CassandraSession) -> "CassandraSchema":
        """Retrieve the schema using CassandraSession"""
        # Cassandra driver updates the schema in the background. Right after connect it's possible the schema hasn't been
        # retrieved yet, and it appears to be empty. This call should block until the schema has been retrieved, and raise
        # an exception if the nodes aren't in sync
        cas.cluster_refresh_schema_metadata(max_schema_agreement_wait=10.0)

        if not cas.cluster_metadata.keyspaces:
            # there should always be system keyspaces, even when there are no user ones
            raise ValueError("No keyspaces in schema metadata")
        return cls(
            keyspaces=sorted(
                CassandraKeyspace.from_cassandra_metadata(metadata) for metadata in cas.cluster_metadata.keyspaces.values()
            )
        )

    def calculate_hash(self) -> str:
        """Get hash of the schema.

        This works as simply as it does due to schema-reading
        functions (from_cassandra*) enforcing ordering by name.

        Based on code review, use of SELECT schema_version FROM
        system.local is preferrable. This is now used only in tests to
        ensure the dumping in general works.

        """
        return hashlib.sha256(self.json().encode("utf-8")).hexdigest()

    def restore_pre_data(self, cas: CassandraSession) -> None:
        """First part of Cassandra schema restoration.

        Cassandra schema restoration consists of two parts:

        - restore_pre_data is done before any of the snapshot data is
          moved into place, and its goal is mainly to have the schema
          correspond to what was in the backup (this function)

        - restore_post_data is done after the restore_pre_data, and
          actual snapshot restoration + moving of files has been done
        """

        for keyspace in self.keyspaces:
            # These may be version dependant, and in general should be recreated
            # during restore (and-or other configuration applying)
            if is_system_keyspace(keyspace.name):
                continue
            keyspace.restore(cas)

    def restore_post_data(self, cas: CassandraSession) -> None:
        """Second part of Cassandra schema restoration.

        See restore_pre_data for the flow.
        """
        for keyspace in self.keyspaces:
            # These may be version dependant, and in general should be recreated
            # during restore (and-or other configuration applying)
            if is_system_keyspace(keyspace.name):
                continue
            keyspace.restore_post_data(cas)
