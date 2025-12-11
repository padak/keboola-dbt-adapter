"""
Keboola adapter connections module.

This module implements the connection layer for the dbt-keboola adapter,
providing DB-API 2.0 compatible interfaces over the Keboola Query Service REST API.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Any, Tuple, Dict
from contextlib import contextmanager
import logging

import agate
from dbt.adapters.sql import SQLConnectionManager
from dbt.adapters.contracts.connection import (
    Credentials,
    Connection,
    AdapterResponse,
    ConnectionState,
)
from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError

from keboola_query_service import Client, QueryResult, Column
from keboola_query_service.exceptions import (
    QueryServiceError,
    JobError,
    JobTimeoutError,
    AuthenticationError,
)

logger = logging.getLogger(__name__)


@dataclass
class KeboolaCredentials(Credentials):
    """
    Credentials for connecting to Keboola Query Service.

    Attributes:
        database: Database name (inherited from base, use workspace default)
        schema: Schema name (inherited from base, use workspace default)
        token: Keboola Storage API token (required)
        workspace_id: Keboola workspace ID (required)
        branch_id: Keboola branch ID (default: "default" for production)
        host: Query Service API host (default: "query.keboola.com")
        timeout: Query execution timeout in seconds (default: 300)
    """

    token: str = ""
    workspace_id: str = ""
    branch_id: str = "default"
    host: str = "query.keboola.com"
    timeout: int = 300

    _ALIASES: dict = field(default_factory=lambda: {
        "project": "database",
        "dataset": "schema",
    })

    @property
    def type(self) -> str:
        """Return the adapter type."""
        return "keboola"

    @property
    def unique_field(self) -> str:
        """Return the unique field for hashing credentials."""
        return self.workspace_id

    def _connection_keys(self) -> Tuple[str, ...]:
        """
        Return keys displayed in `dbt debug` output.

        Note: 'token' is intentionally excluded to avoid exposing
        sensitive credentials in logs and console output.
        """
        return (
            "workspace_id",
            "branch_id",
            "host",
            "database",
            "schema",
        )


class KeboolaCursor:
    """
    DB-API 2.0 cursor emulation over Keboola Query Service REST API.

    This cursor provides a standard Python DB-API interface for executing
    queries through the Keboola Query Service, which uses REST API instead
    of a traditional database connection.
    """

    def __init__(
        self,
        client: Client,
        workspace_id: str,
        branch_id: str,
        timeout: int = 300,
    ):
        """
        Initialize cursor.

        Args:
            client: Keboola Query Service client
            workspace_id: Keboola workspace ID
            branch_id: Keboola branch ID
            timeout: Query timeout in seconds
        """
        self._client = client
        self._workspace_id = workspace_id
        self._branch_id = branch_id
        self._timeout = timeout

        # Cursor state
        self._result: Optional[QueryResult] = None
        self._data: List[List[Any]] = []
        self._position = 0
        self._last_query_id: Optional[str] = None

    @property
    def description(self) -> Optional[List[Tuple]]:
        """
        Return column descriptions.

        Returns a list of 7-element tuples as per DB-API 2.0 spec:
        (name, type_code, display_size, internal_size, precision, scale, null_ok)
        """
        if not self._result or not self._result.columns:
            return None

        return [
            (
                col.name,
                col.type,
                None,  # display_size
                col.length,  # internal_size
                None,  # precision
                None,  # scale
                col.nullable,
            )
            for col in self._result.columns
        ]

    @property
    def rowcount(self) -> int:
        """Return number of rows affected/returned by last query."""
        if not self._result:
            return -1

        # For SELECT queries, return number of rows
        if self._result.number_of_rows is not None:
            return self._result.number_of_rows

        # For DML queries (INSERT, UPDATE, DELETE), return rows affected
        if self._result.rows_affected is not None:
            return self._result.rows_affected

        return len(self._data)

    @property
    def status_message(self) -> Optional[str]:
        """Return status message from last query."""
        if not self._result:
            return None
        return self._result.message

    def execute(self, sql: str, bindings: Optional[List[Any]] = None) -> None:
        """
        Execute SQL query through Keboola Query Service.

        Args:
            sql: SQL statement to execute
            bindings: Optional query parameters (not currently supported)

        Raises:
            DbtDatabaseError: If query execution fails
        """
        if bindings:
            logger.warning("Parameter bindings are not supported by Keboola Query Service")

        try:
            logger.debug(f"Executing query: {sql[:200]}...")

            # Execute query through Keboola Query Service
            results = self._client.execute_query(
                branch_id=self._branch_id,
                workspace_id=self._workspace_id,
                statements=[sql],
                transactional=True,
                max_wait_time=float(self._timeout),
            )

            # Store first result (we only execute one statement at a time)
            if results:
                self._result = results[0]
                self._data = self._result.data or []
                self._position = 0

                logger.debug(
                    f"Query completed: {self.rowcount} rows, "
                    f"status: {self._result.status}"
                )
            else:
                self._result = None
                self._data = []
                self._position = 0

        except AuthenticationError as e:
            raise DbtDatabaseError(f"Authentication failed: {e}") from e
        except JobTimeoutError as e:
            raise DbtDatabaseError(f"Query timeout after {self._timeout}s: {e}") from e
        except JobError as e:
            raise DbtDatabaseError(f"Query execution failed: {e}") from e
        except QueryServiceError as e:
            raise DbtDatabaseError(f"Query service error: {e}") from e
        except Exception as e:
            raise DbtDatabaseError(f"Unexpected error executing query: {e}") from e

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        """
        Fetch next row from query results.

        Returns:
            Tuple containing row data, or None if no more rows
        """
        if self._position >= len(self._data):
            return None

        row = tuple(self._data[self._position])
        self._position += 1
        return row

    def fetchmany(self, size: int = 1) -> List[Tuple[Any, ...]]:
        """
        Fetch next `size` rows from query results.

        Args:
            size: Number of rows to fetch

        Returns:
            List of tuples containing row data
        """
        end_position = min(self._position + size, len(self._data))
        rows = [tuple(row) for row in self._data[self._position:end_position]]
        self._position = end_position
        return rows

    def fetchall(self) -> List[Tuple[Any, ...]]:
        """
        Fetch all remaining rows from query results.

        Returns:
            List of tuples containing row data
        """
        rows = [tuple(row) for row in self._data[self._position:]]
        self._position = len(self._data)
        return rows

    def close(self) -> None:
        """Close the cursor and clean up resources."""
        self._result = None
        self._data = []
        self._position = 0


class KeboolaConnectionHandle:
    """
    Wrapper for Keboola Query Service client.

    This class wraps the SDK Client and provides a DB-API compatible
    interface for dbt to interact with.
    """

    def __init__(
        self,
        client: Client,
        workspace_id: str,
        branch_id: str,
        timeout: int = 300,
    ):
        """
        Initialize connection handle.

        Args:
            client: Keboola Query Service client
            workspace_id: Keboola workspace ID
            branch_id: Keboola branch ID
            timeout: Query timeout in seconds
        """
        self._client = client
        self.workspace_id = workspace_id
        self.branch_id = branch_id
        self.timeout = timeout
        self._in_transaction = False

    def cursor(self) -> KeboolaCursor:
        """
        Create a new cursor for executing queries.

        Returns:
            KeboolaCursor instance
        """
        return KeboolaCursor(
            client=self._client,
            workspace_id=self.workspace_id,
            branch_id=self.branch_id,
            timeout=self.timeout,
        )

    def begin(self) -> None:
        """Begin a transaction (no-op for Keboola REST API)."""
        self._in_transaction = True
        logger.debug("Transaction started (simulated)")

    def commit(self) -> None:
        """Commit a transaction (no-op for Keboola REST API)."""
        self._in_transaction = False
        logger.debug("Transaction committed (simulated)")

    def rollback(self) -> None:
        """Rollback a transaction (no-op for Keboola REST API)."""
        self._in_transaction = False
        logger.debug("Transaction rolled back (simulated)")

    def close(self) -> None:
        """Close the connection and clean up resources."""
        try:
            self._client.close()
        except Exception as e:
            logger.warning(f"Error closing Keboola client: {e}")


class KeboolaConnectionManager(SQLConnectionManager):
    """
    Connection manager for Keboola adapter.

    Manages the lifecycle of connections to Keboola Query Service,
    including opening, closing, and executing queries.
    """

    TYPE = "keboola"

    @contextmanager
    def exception_handler(self, sql: str):
        """
        Context manager for handling query exceptions.

        Args:
            sql: SQL being executed (for error context)
        """
        try:
            yield
        except AuthenticationError as e:
            raise DbtDatabaseError(f"Authentication failed: {e}") from e
        except JobTimeoutError as e:
            raise DbtDatabaseError(f"Query timeout: {e}") from e
        except JobError as e:
            raise DbtDatabaseError(f"Query failed: {e}") from e
        except QueryServiceError as e:
            raise DbtDatabaseError(f"Query service error: {e}") from e
        except Exception as e:
            logger.debug(f"Error while running:\n{sql}")
            raise DbtDatabaseError(str(e)) from e

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        """
        Open a connection to Keboola Query Service.

        Args:
            connection: Connection object with credentials

        Returns:
            Connection object with opened handle

        Raises:
            DbtRuntimeError: If credentials are invalid
            DbtDatabaseError: If connection fails
        """
        if connection.state == ConnectionState.OPEN:
            logger.debug("Connection already open, skipping open")
            return connection

        credentials: KeboolaCredentials = connection.credentials

        # Validate required credentials
        if not credentials.token:
            raise DbtRuntimeError("Missing required credential: token")
        if not credentials.workspace_id:
            raise DbtRuntimeError("Missing required credential: workspace_id")

        try:
            # Construct base URL
            base_url = f"https://{credentials.host}"

            logger.debug(
                f"Opening connection to Keboola Query Service: "
                f"workspace={credentials.workspace_id}, "
                f"branch={credentials.branch_id}"
            )

            # Create Keboola Query Service client
            client = Client(
                base_url=base_url,
                token=credentials.token,
                timeout=float(credentials.timeout),
                connect_timeout=10.0,
                max_retries=3,
            )

            # Create connection handle
            handle = KeboolaConnectionHandle(
                client=client,
                workspace_id=credentials.workspace_id,
                branch_id=credentials.branch_id,
                timeout=credentials.timeout,
            )

            connection.handle = handle
            connection.state = ConnectionState.OPEN

            logger.debug("Connection opened successfully")

        except AuthenticationError as e:
            raise DbtDatabaseError(
                f"Failed to authenticate with Keboola: {e}. "
                f"Please check your token."
            ) from e
        except Exception as e:
            raise DbtDatabaseError(
                f"Failed to connect to Keboola Query Service: {e}"
            ) from e

        return connection

    def cancel(self, connection: Connection) -> None:
        """
        Cancel any running queries on this connection.

        Args:
            connection: Connection to cancel queries on
        """
        # Note: The Keboola Query Service SDK doesn't provide a way to
        # get the current job ID from a connection, so we can't cancel
        # queries directly. This is a limitation of the REST API approach.
        logger.debug("Query cancellation not implemented for Keboola adapter")

    def begin(self) -> None:
        """Begin a transaction (simulated for Keboola REST API)."""
        connection = self.get_thread_connection()
        if connection.handle:
            connection.handle.begin()

    def commit(self) -> None:
        """Commit a transaction (simulated for Keboola REST API)."""
        connection = self.get_thread_connection()
        if connection.handle:
            connection.handle.commit()

    def rollback(self) -> None:
        """Rollback a transaction (simulated for Keboola REST API)."""
        connection = self.get_thread_connection()
        if connection.handle:
            connection.handle.rollback()

    def get_response(self, cursor: KeboolaCursor) -> AdapterResponse:
        """
        Get adapter response from cursor.

        Args:
            cursor: Cursor that executed a query

        Returns:
            AdapterResponse with query metadata
        """
        message = cursor.status_message or "OK"
        rows = cursor.rowcount

        return AdapterResponse(
            _message=message,
            rows_affected=rows,
        )

    def execute(
        self,
        sql: str,
        auto_begin: bool = False,
        fetch: bool = False,
        limit: Optional[int] = None,
    ) -> Tuple[AdapterResponse, agate.Table]:
        """
        Execute SQL and return results.

        Args:
            sql: SQL to execute
            auto_begin: Whether to auto-begin transaction (ignored for Keboola)
            fetch: Whether to fetch results
            limit: Optional limit on rows to fetch

        Returns:
            Tuple of (AdapterResponse, agate.Table with results)
        """
        connection = self.get_thread_connection()
        cursor = connection.handle.cursor()

        try:
            # Execute query
            cursor.execute(sql)

            # Get response
            response = self.get_response(cursor)

            # Fetch results if requested
            if fetch:
                table = self.get_result_from_cursor(cursor, limit=limit)
            else:
                table = agate.Table(rows=[])

            return response, table

        finally:
            cursor.close()

    def get_result_from_cursor(
        self, cursor: KeboolaCursor, limit: Optional[int] = None
    ) -> agate.Table:
        """
        Convert cursor results to agate.Table.

        Args:
            cursor: Cursor with query results
            limit: Optional limit on rows to return

        Returns:
            agate.Table containing results
        """
        # Get column names and types from cursor description
        if cursor.description:
            column_names = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            if limit is not None:
                rows = rows[:limit]
        else:
            column_names = []
            rows = []

        # Create agate table
        # Note: agate will auto-detect column types from the data
        return agate.Table(rows=rows, column_names=column_names)
