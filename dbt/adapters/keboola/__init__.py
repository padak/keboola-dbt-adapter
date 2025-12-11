from dbt.adapters.base import AdapterPlugin
from dbt.adapters.keboola.connections import KeboolaCredentials, KeboolaConnectionManager
from dbt.adapters.keboola.impl import KeboolaAdapter
from dbt.adapters.keboola.column import KeboolaColumn
from dbt.adapters.keboola.relation import KeboolaRelation
from dbt.include import keboola

__version__ = "0.1.0"

Plugin = AdapterPlugin(
    adapter=KeboolaAdapter,
    credentials=KeboolaCredentials,
    include_path=keboola.PACKAGE_PATH,
)

__all__ = [
    "Plugin",
    "KeboolaAdapter",
    "KeboolaCredentials",
    "KeboolaConnectionManager",
    "KeboolaColumn",
    "KeboolaRelation",
]
