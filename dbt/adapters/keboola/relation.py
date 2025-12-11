from dataclasses import dataclass
from typing import FrozenSet, Optional, Type

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.adapters.contracts.relation import ComponentName


@dataclass(frozen=True, eq=False, repr=False)
class KeboolaRelation(BaseRelation):
    """
    Relation class for Keboola adapter.
    Uses Snowflake-style quoting (double quotes for identifiers).
    Case-insensitive matching like Snowflake.
    """

    quote_character: str = '"'

    def _is_exactish_match(self, other: "KeboolaRelation") -> bool:
        """
        Case-insensitive comparison for Snowflake.
        """
        if other is None:
            return False
        return (
            self.database is not None
            and other.database is not None
            and self.database.upper() == other.database.upper()
            and self.schema is not None
            and other.schema is not None
            and self.schema.upper() == other.schema.upper()
            and self.identifier is not None
            and other.identifier is not None
            and self.identifier.upper() == other.identifier.upper()
        )

    def matches(
        self,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        identifier: Optional[str] = None,
    ) -> bool:
        """
        Case-insensitive matching for Snowflake-style identifiers.
        """
        search = filter(None, [database, schema, identifier])

        if not any([self.database, self.schema, self.identifier]):
            return False

        for part in search:
            part_upper = part.upper()
            if self.database and self.database.upper() == part_upper:
                continue
            if self.schema and self.schema.upper() == part_upper:
                continue
            if self.identifier and self.identifier.upper() == part_upper:
                continue
            return False
        return True

    @classmethod
    def get_default_quote_policy(cls) -> Policy:
        """
        Define the default quote policy for Keboola relations.
        Snowflake-style: quote identifiers, not database/schema.
        """
        return Policy(
            database=False,
            schema=False,
            identifier=True,
        )

    @classmethod
    def get_default_include_policy(cls) -> Policy:
        """
        Define the default include policy for Keboola relations.
        Include database, schema, and identifier in relation paths.
        """
        return Policy(
            database=True,
            schema=True,
            identifier=True,
        )

    def render(self) -> str:
        """
        Render the relation as a fully qualified name.
        Returns format like: database.schema."identifier"
        """
        parts = []

        include_policy = self.include_policy or self.get_default_include_policy()
        quote_policy = self.quote_policy or self.get_default_quote_policy()

        if include_policy.database and self.database:
            database = self.database
            if quote_policy.database:
                database = f'{self.quote_character}{database}{self.quote_character}'
            parts.append(database)

        if include_policy.schema and self.schema:
            schema = self.schema
            if quote_policy.schema:
                schema = f'{self.quote_character}{schema}{self.quote_character}'
            parts.append(schema)

        if include_policy.identifier and self.identifier:
            identifier = self.identifier
            if quote_policy.identifier:
                identifier = f'{self.quote_character}{identifier}{self.quote_character}'
            parts.append(identifier)

        return '.'.join(parts)

    def quote(
        self,
        database: Optional[bool] = None,
        schema: Optional[bool] = None,
        identifier: Optional[bool] = None,
    ) -> "KeboolaRelation":
        """
        Return a new relation with updated quote policy.
        """
        quote_policy = self.quote_policy or self.get_default_quote_policy()

        return self.replace(
            quote_policy=quote_policy.replace(
                database=database if database is not None else quote_policy.database,
                schema=schema if schema is not None else quote_policy.schema,
                identifier=identifier if identifier is not None else quote_policy.identifier,
            )
        )

    def include(
        self,
        database: Optional[bool] = None,
        schema: Optional[bool] = None,
        identifier: Optional[bool] = None,
    ) -> "KeboolaRelation":
        """
        Return a new relation with updated include policy.
        """
        include_policy = self.include_policy or self.get_default_include_policy()

        return self.replace(
            include_policy=include_policy.replace(
                database=database if database is not None else include_policy.database,
                schema=schema if schema is not None else include_policy.schema,
                identifier=identifier if identifier is not None else include_policy.identifier,
            )
        )
