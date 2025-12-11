# dbt Adapter pro Keboola Query Service - Implementation Plan

## Cil
Vytvorit plnohodnotny dbt adapter, ktery umozni spoustet dbt modely pres Keboola Query Service REST API misto primeho pripojeni k Snowflake.

## Status: FUNKCNI ZAKLAD

Adapter funguje pro zakladni use case:
- `dbt debug` - OK
- `dbt run` s table materializaci - OK
- Idempotentni operace - OK
- Cteni z read-only schemat - OK

## Klicove informace
- **Backend:** Snowflake (SQL dialekt)
- **SDK:** `keboola-query-service` Python package
- **Test data:** `out.c-amplitude.events`
- **Branch ID:** Ciselne ID (ne "default")

## Struktura projektu

```
dbt-keboola/
├── pyproject.toml                    # Python packaging
├── requirements.txt
├── CLAUDE.md                         # Dokumentace
├── PLAN.md                           # Tento soubor
├── test_query_service.py             # Test script pro SDK
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
└── sample_project/                   # Testovaci dbt projekt
    ├── dbt_project.yml               # S dispatch konfiguraci
    ├── profiles.yml
    └── models/
        └── test_simple.sql           # Funkcni test model
```

## Implementacni kroky

### 1. Zakladni struktura projektu
- [x] Vytvorit adresarovou strukturu
- [x] Nastavit pyproject.toml s dependencies
- [x] Vytvorit requirements.txt
- [x] Namespace package __init__.py soubory

### 2. Core tridy (connections.py)
- [x] `KeboolaCredentials` - token, workspace_id, branch_id, host
- [x] `KeboolaConnectionManager` - sprava API klienta
- [x] `KeboolaConnectionHandle` - wrapper pro SDK Client s transaction metodami
- [x] `KeboolaCursor` - DB-API 2.0 emulace nad REST API

### 3. Adapter implementace (impl.py)
- [x] `KeboolaAdapter(SQLAdapter)` - hlavni adapter trida
- [x] `list_relations_without_caching()` - seznam tabulek
- [x] `get_columns_in_relation()` - schema tabulky
- [x] `drop_relation()`, `truncate_relation()`, `rename_relation()`
- [x] `create_schema()`, `drop_schema()`

### 4. Pomocne tridy
- [x] `KeboolaRelation` (relation.py) - Snowflake naming, case-insensitive matching
- [x] `KeboolaColumn` (column.py) - Snowflake type mappings

### 5. Macro implementace
- [x] `keboola__create_table_as` - CREATE OR REPLACE TABLE
- [x] `keboola__create_view_as` - CREATE OR REPLACE VIEW
- [x] `keboola__get_create_table_as_sql` - dispatched verze
- [x] Table materialization (zakladni)
- [x] Catalog/information_schema queries (uppercase columns)
- [ ] View materialization (potrebuje test)
- [ ] Incremental materialization (merge, delete+insert, append)
- [ ] Seeds loading
- [ ] Snapshot materialization

### 6. Plugin registrace
- [x] `__init__.py` s AdapterPlugin
- [x] Entry point v pyproject.toml

### 7. Testovaci dbt projekt
- [x] profiles.yml s keboola credentials
- [x] dbt_project.yml s dispatch konfiguraci
- [x] Test model `test_simple.sql` nad amplitude events
- [ ] Dalsi testovaci modely

### 8. Testy
- [ ] Unit testy pro credentials a cursor
- [ ] Funkcni testy s realnym API

## Klicove opravy behem vyvoje

| Problem | Reseni |
|---------|--------|
| 403 workspace error | Pouzit ID z `/v2/storage/workspaces` API, ne ze SNOWFLAKE_USER |
| Case sensitivity | Implementovat `_is_exactish_match()` a `matches()` v KeboolaRelation |
| Makra se nepouzivaji | Pridat `dispatch` sekci do dbt_project.yml |
| INFORMATION_SCHEMA columns | Pouzit UPPERCASE bez uvozovek |
| Transaction errors | Pridat `begin()`, `commit()`, `rollback()` metody |

## Konfigurace

### .env
```
KEBOOLA_API_TOKEN=xxx
KEBOOLA_WORKSPACE_ID=2950196630    # Z /v2/storage/workspaces
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

## Spusteni

```bash
# Aktivovat prostredi
source .venv/bin/activate
set -a && source .env && set +a

# Test pripojeni
dbt debug --project-dir sample_project --profiles-dir sample_project

# Spustit model
dbt run --project-dir sample_project --profiles-dir sample_project --select test_simple
```

## Dalsi kroky (TODO)

1. **View materializace** - otestovat a pripadne opravit
2. **Incremental materializace** - implementovat merge strategii
3. **Unit testy** - pytest pro connections, cursor, relation
4. **Functional testy** - testy s realnym Keboola API
5. **CI/CD** - GitHub Actions pro automaticke testovani
6. **Dokumentace** - README.md pro publikaci na PyPI
