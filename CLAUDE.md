# dbt-keboola Adapter

dbt adapter for Keboola Query Service - enables running dbt models through Keboola REST API instead of direct Snowflake connection.

## Quick Start

```bash
# Activate environment
source .venv/bin/activate
set -a && source .env && set +a

# Test connection
dbt debug --project-dir sample_project --profiles-dir sample_project

# Run model
dbt run --project-dir sample_project --profiles-dir sample_project --select test_simple

# Run all models
dbt run --project-dir sample_project --profiles-dir sample_project
```

## Project Structure

```
dbt-keboola/
├── dbt/
│   ├── adapters/keboola/
│   │   ├── __init__.py          # Plugin registration
│   │   ├── connections.py       # KeboolaCredentials, KeboolaConnectionManager, KeboolaCursor
│   │   ├── impl.py              # KeboolaAdapter
│   │   ├── relation.py          # KeboolaRelation (Snowflake naming, case-insensitive)
│   │   └── column.py            # KeboolaColumn (Snowflake types)
│   └── include/keboola/
│       ├── dbt_project.yml
│       └── macros/              # SQL macros for materializations
├── sample_project/              # Test dbt project
├── test_query_service.py        # Test script for Query Service SDK
├── pyproject.toml
└── requirements.txt
```

## Installation

```bash
# Create venv with Python 3.13 (NOT 3.14 - incompatible with dbt)
python3.13 -m venv .venv
source .venv/bin/activate

# Install adapter
pip install .

# For development (editable install doesn't work well with namespace packages)
pip install . --force-reinstall --no-deps
```

## Configuration

### .env file
```
KEBOOLA_API_TOKEN=your-storage-api-token
KEBOOLA_WORKSPACE_ID=numeric-workspace-id       # From API: GET /v2/storage/workspaces
KEBOOLA_BRANCH_ID=numeric-branch-id
KEBOOLA_SNOWFLAKE_DB=SAPI_xxxxx
KEBOOLA_SNOWFLAKE_SCHEMA=WORKSPACE_xxxxx
```

**IMPORTANT:** `KEBOOLA_WORKSPACE_ID` must be the ID from Storage API (`/v2/storage/workspaces`), not the number from `SNOWFLAKE_USER`!

### profiles.yml
```yaml
keboola_project:
  target: dev
  outputs:
    dev:
      type: keboola
      database: "{{ env_var('KEBOOLA_SNOWFLAKE_DB') }}"
      schema: "{{ env_var('KEBOOLA_SNOWFLAKE_SCHEMA') }}"
      token: "{{ env_var('KEBOOLA_API_TOKEN') }}"
      workspace_id: "{{ env_var('KEBOOLA_WORKSPACE_ID') }}"
      branch_id: "{{ env_var('KEBOOLA_BRANCH_ID') }}"
      host: "query.keboola.com"
      timeout: 300
      threads: 4
```

### dbt_project.yml (dispatch configuration)
To properly use Keboola macros add:
```yaml
dispatch:
  - macro_namespace: dbt
    search_order: ['keboola', 'dbt']
```

## Keboola Workspace vs Schema

- **Workspace schema** (e.g., `WORKSPACE_1282429287`) = **writable** - dbt writes tables here
- **Other schemas** (e.g., `out.c-amplitude`) = **read-only** - source data

Example SQL query for source data:
```sql
SELECT "platform", "event_type", count("event_id") as "events"
FROM "SAPI_10504"."out.c-amplitude"."events"
GROUP BY 1,2 ORDER BY 3 DESC;
```

## Key Implementation Details

### Namespace packages
Files `dbt/__init__.py` and `dbt/adapters/__init__.py` MUST contain:
```python
__path__ = __import__('pkgutil').extend_path(__path__, __name__)
```
Otherwise the local package will override the installed dbt.

### Case-insensitive matching
Snowflake stores identifiers in UPPERCASE. `KeboolaRelation` implements:
- `_is_exactish_match()` - case-insensitive relation comparison
- `matches()` - case-insensitive search

### CREATE OR REPLACE
Macro `keboola__get_create_table_as_sql` uses `CREATE OR REPLACE TABLE` for idempotent operations.

### Transactions (simulated)
REST API doesn't support transactions, methods `begin()`, `commit()`, `rollback()` are no-op.

### KeboolaCredentials
- Inherits from `dbt.adapters.contracts.connection.Credentials`
- Base class has `database` and `schema` as required fields
- All our fields must have defaults (otherwise dataclass error)

### KeboolaCursor
- Emulates DB-API 2.0 cursor over REST API
- `execute()` calls `client.execute_query()` from keboola-query-service SDK
- Results stored in memory, `fetchall()` returns them

## Keboola Query Service API

### Required credentials
- **token**: Keboola Storage API token (starts with project_id-)
- **workspace_id**: NUMERIC workspace ID from `/v2/storage/workspaces` API
- **branch_id**: NUMERIC branch ID (not "default")

### SDK usage
```python
from keboola_query_service import Client

client = Client(base_url="https://query.keboola.com", token=token)
results = client.execute_query(
    branch_id="1261313",
    workspace_id="2950196630",  # ID from Storage API!
    statements=["SELECT 1"],
    transactional=True,
)
```

### How to find correct Workspace ID
```python
import httpx
headers = {'X-StorageApi-Token': token}
resp = httpx.get('https://connection.keboola.com/v2/storage/workspaces', headers=headers)
for ws in resp.json():
    print(f"ID: {ws['id']} - {ws['name']}")
```

## Troubleshooting

### "No module named 'dbt.adapters.keboola'"
- Use non-editable install: `pip install .` (not `pip install -e .`)
- Verify namespace package __init__.py files

### "Failed to parse branch ID"
- branch_id must be numeric, not "default"
- Find it in Keboola UI: Settings > Branches

### "Failed to get workspace credentials (403)"
- Verify workspace_id is from `/v2/storage/workspaces` API
- Verify token has access to workspace
- Verify workspace is on correct branch

### "approximate match" / case sensitivity error
- Snowflake is case-insensitive, but dbt searches case-sensitive
- `KeboolaRelation` must implement `_is_exactish_match()` and `matches()`

### Python 3.14 errors
- dbt is not compatible with Python 3.14
- Use Python 3.13 or 3.11

## Status

- [x] Basic adapter works
- [x] `dbt debug` passes
- [x] `dbt run` with table materialization works
- [x] Idempotent operations (repeated runs)
- [x] Reading from read-only schemas (out.c-amplitude)
- [ ] View materialization
- [ ] Incremental materialization
- [ ] Snapshot materialization
- [ ] Unit tests
