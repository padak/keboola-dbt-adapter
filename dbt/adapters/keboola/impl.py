from typing import Optional, List, Set, Any, Dict
import logging
import agate

from dbt.adapters.sql import SQLAdapter

logger = logging.getLogger(__name__)
from dbt.adapters.base.meta import available
from dbt.adapters.base import AdapterConfig
from dbt.adapters.base.relation import BaseRelation

from dbt.adapters.keboola.connections import KeboolaConnectionManager
from dbt.adapters.keboola.relation import KeboolaRelation
from dbt.adapters.keboola.column import KeboolaColumn


class KeboolaAdapter(SQLAdapter):
    """
    Adapter for Keboola Connection.
    Uses Snowflake-based SQL dialect through Keboola's Query Service API.
    """

    ConnectionManager = KeboolaConnectionManager
    Relation = KeboolaRelation
    Column = KeboolaColumn

    @classmethod
    def date_function(cls) -> str:
        """
        Return the current timestamp function.
        Snowflake uses CURRENT_TIMESTAMP().
        """
        return "CURRENT_TIMESTAMP()"

    @available
    def is_cancelable(cls) -> bool:
        """
        Indicate that queries can be cancelled.
        """
        return True

    def quote(self, identifier: str) -> str:
        """
        Quote an identifier using Snowflake-style double quotes.
        """
        return f'"{identifier}"'

    @available
    def list_relations_without_caching(
        self, schema_relation: KeboolaRelation
    ) -> List[KeboolaRelation]:
        """
        List all relations in a schema without using the cache.
        Queries INFORMATION_SCHEMA.TABLES.
        """
        kwargs = {
            "schema_relation": schema_relation,
        }

        try:
            results = self.execute_macro(
                "list_relations_without_caching",
                kwargs=kwargs,
            )
        except Exception as e:
            logger.debug(
                f"Error listing relations in schema {schema_relation}: {e}"
            )
            return []

        relations = []
        for row in results:
            if len(row) < 4:
                continue

            database, schema, identifier, relation_type = row[:4]

            relation = self.Relation.create(
                database=database,
                schema=schema,
                identifier=identifier,
                type=relation_type,
            )
            relations.append(relation)

        return relations

    @available
    def get_columns_in_relation(self, relation: KeboolaRelation) -> List[KeboolaColumn]:
        """
        Get all columns in a relation.
        Queries INFORMATION_SCHEMA.COLUMNS.
        """
        try:
            rows = self.execute_macro(
                "get_columns_in_relation",
                kwargs={"relation": relation},
            )
        except Exception as e:
            logger.debug(
                f"Error getting columns for relation {relation}: {e}"
            )
            return []

        columns = []
        for row in rows:
            column = KeboolaColumn(
                column=row.column_name,
                dtype=row.data_type,
                char_size=getattr(row, 'character_maximum_length', None),
                numeric_precision=getattr(row, 'numeric_precision', None),
                numeric_scale=getattr(row, 'numeric_scale', None),
            )
            columns.append(column)

        return columns

    @available
    def drop_relation(self, relation: KeboolaRelation) -> None:
        """
        Drop a relation (table or view).
        """
        if relation.type is None:
            relation = relation.incorporate(type="table")

        self.execute_macro(
            "drop_relation",
            kwargs={"relation": relation},
        )

    @available
    def truncate_relation(self, relation: KeboolaRelation) -> None:
        """
        Truncate a table.
        """
        self.execute_macro(
            "truncate_relation",
            kwargs={"relation": relation},
        )

    @available
    def rename_relation(
        self, from_relation: KeboolaRelation, to_relation: KeboolaRelation
    ) -> None:
        """
        Rename a relation.
        """
        self.execute_macro(
            "rename_relation",
            kwargs={
                "from_relation": from_relation,
                "to_relation": to_relation,
            },
        )

    @available
    def create_schema(self, relation: KeboolaRelation) -> None:
        """
        Create a schema.
        """
        relation = relation.without_identifier()
        self.execute_macro(
            "create_schema",
            kwargs={"relation": relation},
        )

    @available
    def drop_schema(self, relation: KeboolaRelation) -> None:
        """
        Drop a schema.
        """
        relation = relation.without_identifier()
        self.execute_macro(
            "drop_schema",
            kwargs={"relation": relation},
        )

    @available
    def list_schemas(self, database: str) -> List[str]:
        """
        List all schemas in a database.
        """
        results = self.execute_macro(
            "list_schemas",
            kwargs={"database": database},
        )

        schemas = []
        for row in results:
            schemas.append(row[0])

        return schemas

    @available
    def check_schema_exists(self, database: str, schema: str) -> bool:
        """
        Check if a schema exists.
        """
        results = self.execute_macro(
            "check_schema_exists",
            kwargs={"database": database, "schema": schema},
        )

        return len(results) > 0

    def timestamp_add_sql(self, add_to: str, number: int = 1, interval: str = "hour") -> str:
        """
        Generate SQL for timestamp addition.
        Snowflake uses DATEADD function.
        """
        return f"DATEADD({interval}, {number}, {add_to})"

    def string_add_sql(
        self,
        add_to: str,
        value: str,
        location: str = "append",
    ) -> str:
        """
        Generate SQL for string concatenation.
        """
        if location == "append":
            return f"{add_to} || '{value}'"
        elif location == "prepend":
            return f"'{value}' || {add_to}"
        else:
            raise ValueError(f"Invalid location: {location}")

    @available
    def get_catalog(self, manifest: Any) -> agate.Table:
        """
        Get catalog information for all relations.
        """
        schema_map = self._get_catalog_schemas(manifest)

        if len(schema_map) > 1:
            raise ValueError(
                f"Expected exactly one schema in get_catalog, found {len(schema_map)}"
            )

        with self.connection_named("list_catalog"):
            results = []
            for info, schemas in schema_map.items():
                for schema in schemas:
                    relations = self.list_relations_without_caching(
                        schema_relation=self.Relation.create(
                            database=info.database,
                            schema=schema,
                        )
                    )

                    for relation in relations:
                        columns = self.get_columns_in_relation(relation)
                        for column in columns:
                            results.append({
                                "table_database": relation.database,
                                "table_schema": relation.schema,
                                "table_name": relation.identifier,
                                "table_type": relation.type,
                                "column_name": column.name,
                                "column_index": column.index if hasattr(column, 'index') else None,
                                "column_type": column.data_type,
                            })

            return agate.Table.from_object(results)

    @available
    def valid_snapshot_target(self, relation: KeboolaRelation) -> None:
        """
        Verify that the relation is a valid snapshot target.
        """
        if relation.type not in ("table", "view"):
            raise ValueError(
                f"Snapshot target must be a table or view, got {relation.type}"
            )

    def standardize_grants_dict(self, grants_dict: Dict[str, List[str]]) -> Dict[str, List[str]]:
        """
        Standardize grant privileges for Snowflake.
        """
        standardized = {}
        for grantee, privileges in grants_dict.items():
            standardized[grantee] = [p.upper() for p in privileges]
        return standardized
