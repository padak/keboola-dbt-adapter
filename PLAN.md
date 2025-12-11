# dbt Adapter for Keboola Query Service - Implementation Plan

## Goal
Create a full-featured dbt adapter that enables running dbt models through Keboola Query Service REST API instead of direct Snowflake connection.

## Status: FUNCTIONAL BASE

The adapter works for basic use cases:
- `dbt debug` - OK
- `dbt run` with table materialization - OK
- Idempotent operations - OK
- Reading from read-only schemas - OK

## Key Information
- **Backend:** Snowflake (SQL dialect)
- **SDK:** `keboola-query-service` Python package
- **Test data:** `out.c-amplitude.events`
- **Branch ID:** Numeric ID (not "default")

## Project Structure

```
dbt-keboola/
├── pyproject.toml                    # Python packaging
├── requirements.txt
├── CLAUDE.md                         # Documentation
├── PLAN.md                           # This file
├── test_query_service.py             # Test script for SDK
│
├── dbt/
│   ├── __init__.py                   # Namespace package
│   ├── adapters/
│   │   ├── __init__.py               # Namespace package
│   │   └── keboola/
│   │       ├── __init__.py           # AdapterPlugin registration
│   │       ├── connections.py        # Credentials + ConnectionManager + Cursor
│   │       ├── impl.py               # KeboolaAdapter
│   │       ├── relation.py           # KeboolaRelation (case-insensitive)
│   │       └── column.py             # KeboolaColumn
│   └── include/
│       └── keboola/
│           ├── __init__.py
│           ├── dbt_project.yml
│           └── macros/
│               ├── adapters.sql      # CREATE OR REPLACE, etc.
│               ├── catalog.sql       # INFORMATION_SCHEMA queries
│               └── materializations/
│                   ├── table.sql
│                   ├── view.sql
│                   └── incremental.sql
│
├── tests/                            # TODO
│
└── sample_project/                   # Test dbt project
    ├── dbt_project.yml               # With dispatch configuration
    ├── profiles.yml
    └── models/
        └── test_simple.sql           # Functional test model
```

## Implementation Steps

### 1. Basic Project Structure
- [x] Create directory structure
- [x] Set up pyproject.toml with dependencies
- [x] Create requirements.txt
- [x] Namespace package __init__.py files

### 2. Core Classes (connections.py)
- [x] `KeboolaCredentials` - token, workspace_id, branch_id, host
- [x] `KeboolaConnectionManager` - API client management
- [x] `KeboolaConnectionHandle` - wrapper for SDK Client with transaction methods
- [x] `KeboolaCursor` - DB-API 2.0 emulation over REST API

### 3. Adapter Implementation (impl.py)
- [x] `KeboolaAdapter(SQLAdapter)` - main adapter class
- [x] `list_relations_without_caching()` - list tables
- [x] `get_columns_in_relation()` - table schema
- [x] `drop_relation()`, `truncate_relation()`, `rename_relation()`
- [x] `create_schema()`, `drop_schema()`

### 4. Helper Classes
- [x] `KeboolaRelation` (relation.py) - Snowflake naming, case-insensitive matching
- [x] `KeboolaColumn` (column.py) - Snowflake type mappings

### 5. Macro Implementation
- [x] `keboola__create_table_as` - CREATE OR REPLACE TABLE
- [x] `keboola__create_view_as` - CREATE OR REPLACE VIEW
- [x] `keboola__get_create_table_as_sql` - dispatched version
- [x] Table materialization (basic)
- [x] Catalog/information_schema queries (uppercase columns)
- [ ] View materialization (needs testing)
- [ ] Incremental materialization (merge, delete+insert, append)
- [ ] Seeds loading
- [ ] Snapshot materialization

### 6. Plugin Registration
- [x] `__init__.py` with AdapterPlugin
- [x] Entry point in pyproject.toml

### 7. Test dbt Project
- [x] profiles.yml with keboola credentials
- [x] dbt_project.yml with dispatch configuration
- [x] Test model `test_simple.sql` using amplitude events
- [ ] Additional test models

### 8. Tests
- [ ] Unit tests for credentials and cursor
- [ ] Functional tests with real API

## Key Fixes During Development

| Problem | Solution |
|---------|----------|
| 403 workspace error | Use ID from `/v2/storage/workspaces` API, not from SNOWFLAKE_USER |
| Case sensitivity | Implement `_is_exactish_match()` and `matches()` in KeboolaRelation |
| Macros not being used | Add `dispatch` section to dbt_project.yml |
| INFORMATION_SCHEMA columns | Use UPPERCASE without quotes |
| Transaction errors | Add `begin()`, `commit()`, `rollback()` methods |

## Configuration

### .env
```
KEBOOLA_API_TOKEN=xxx
KEBOOLA_WORKSPACE_ID=2950196630    # From /v2/storage/workspaces
KEBOOLA_BRANCH_ID=1261313
KEBOOLA_SNOWFLAKE_DB=SAPI_10504
KEBOOLA_SNOWFLAKE_SCHEMA=WORKSPACE_1282429287
```

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

### dbt_project.yml
```yaml
dispatch:
  - macro_namespace: dbt
    search_order: ['keboola_sample', 'keboola', 'dbt']
```

## Running

```bash
# Activate environment
source .venv/bin/activate
set -a && source .env && set +a

# Test connection
dbt debug --project-dir sample_project --profiles-dir sample_project

# Run model
dbt run --project-dir sample_project --profiles-dir sample_project --select test_simple
```

## Next Steps (TODO)

1. **View materialization** - test and fix if needed
2. **Incremental materialization** - implement merge strategy
3. **Unit tests** - pytest for connections, cursor, relation
4. **Functional tests** - tests with real Keboola API
5. **CI/CD** - GitHub Actions for automated testing
6. **Documentation** - README.md for PyPI publication
