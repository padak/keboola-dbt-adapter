from dataclasses import dataclass
from typing import ClassVar, Dict, Optional

from dbt.adapters.base.column import Column


@dataclass
class KeboolaColumn(Column):
    """
    Column class for Keboola adapter.
    Snowflake-based type system since Keboola uses Snowflake backend.
    """

    TYPE_LABELS: ClassVar[Dict[str, str]] = {
        "STRING": "VARCHAR",
        "TEXT": "VARCHAR",
        "BINARY": "BINARY",
        "VARBINARY": "VARBINARY",
        "INTEGER": "NUMBER",
        "INT": "NUMBER",
        "BIGINT": "NUMBER",
        "SMALLINT": "NUMBER",
        "TINYINT": "NUMBER",
        "BYTEINT": "NUMBER",
        "NUMERIC": "NUMBER",
        "DECIMAL": "NUMBER",
        "NUMBER": "NUMBER",
        "FLOAT": "FLOAT",
        "FLOAT4": "FLOAT",
        "FLOAT8": "FLOAT",
        "DOUBLE": "FLOAT",
        "DOUBLE PRECISION": "FLOAT",
        "REAL": "FLOAT",
        "BOOLEAN": "BOOLEAN",
        "DATE": "DATE",
        "DATETIME": "TIMESTAMP_NTZ",
        "TIME": "TIME",
        "TIMESTAMP": "TIMESTAMP_NTZ",
        "TIMESTAMP_NTZ": "TIMESTAMP_NTZ",
        "TIMESTAMP_LTZ": "TIMESTAMP_LTZ",
        "TIMESTAMP_TZ": "TIMESTAMP_TZ",
        "VARIANT": "VARIANT",
        "OBJECT": "OBJECT",
        "ARRAY": "ARRAY",
        "GEOGRAPHY": "GEOGRAPHY",
        "GEOMETRY": "GEOMETRY",
    }

    @property
    def data_type(self) -> str:
        """Return the standardized data type."""
        if self.dtype is None:
            return "VARCHAR"
        return self.TYPE_LABELS.get(self.dtype.upper(), self.dtype.upper())

    def is_string(self) -> bool:
        """Check if the column is a string type."""
        return self.data_type.upper() in ("VARCHAR", "CHAR", "CHARACTER", "STRING", "TEXT")

    def is_numeric(self) -> bool:
        """Check if the column is a numeric type."""
        return self.data_type.upper() in ("NUMBER", "NUMERIC", "DECIMAL", "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT")

    def is_integer(self) -> bool:
        """Check if the column is an integer type."""
        return self.data_type.upper() in ("INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT")

    def is_float(self) -> bool:
        """Check if the column is a float type."""
        return self.data_type.upper() in ("FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE PRECISION", "REAL")

    def is_number(self) -> bool:
        """Check if the column is any numeric type (integer or float)."""
        return self.is_numeric() or self.is_float()

    @classmethod
    def string_type(cls, size: Optional[int] = None) -> str:
        """Return the string type definition."""
        if size:
            return f"VARCHAR({size})"
        return "VARCHAR"

    @classmethod
    def numeric_type(cls, dtype: str, precision: Optional[int] = None, scale: Optional[int] = None) -> str:
        """Return the numeric type definition."""
        if dtype.upper() in ("NUMBER", "NUMERIC", "DECIMAL"):
            if precision and scale:
                return f"NUMBER({precision},{scale})"
            elif precision:
                return f"NUMBER({precision})"
            return "NUMBER"
        return dtype.upper()

    def __repr__(self) -> str:
        """Return a string representation of the column."""
        return f"<KeboolaColumn {self.name} ({self.data_type})>"
