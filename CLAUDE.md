# dbt-keboola Adapter

dbt adapter pro Keboola Query Service - umoznuje spoustet dbt modely pres Keboola REST API misto primeho pripojeni k Snowflake.

## Quick Start

```bash
# Aktivovat prostredi
source .venv/bin/activate
set -a && source .env && set +a

# Test pripojeni
dbt debug --project-dir sample_project --profiles-dir sample_project

# Spustit model
dbt run --project-dir sample_project --profiles-dir sample_project --select test_simple

# Spustit vsechny modely
dbt run --project-dir sample_project --profiles-dir sample_project
```

## Struktura projektu

```
dbt-keboola/
├── dbt/
│   ├── adapters/keboola/
│   │   ├── __init__.py          # Plugin registrace
│   │   ├── connections.py       # KeboolaCredentials, KeboolaConnectionManager, KeboolaCursor
│   │   ├── impl.py              # KeboolaAdapter
│   │   ├── relation.py          # KeboolaRelation (Snowflake naming, case-insensitive)
│   │   └── column.py            # KeboolaColumn (Snowflake types)
│   └── include/keboola/
│       ├── dbt_project.yml
│       └── macros/              # SQL macros pro materializace
├── sample_project/              # Testovaci dbt projekt
├── test_query_service.py        # Test script pro Query Service SDK
├── pyproject.toml
└── requirements.txt
```

## Instalace

```bash
# Vytvorit venv s Python 3.13 (NE 3.14 - nekompatibilni s dbt)
python3.13 -m venv .venv
source .venv/bin/activate

# Instalovat adapter
pip install .

# Pro vyvoj (editable install nefunguje dobre s namespace packages)
pip install . --force-reinstall --no-deps
```

## Konfigurace

### .env soubor
```
KEBOOLA_API_TOKEN=your-storage-api-token
KEBOOLA_WORKSPACE_ID=numeric-workspace-id       # Z API: GET /v2/storage/workspaces
KEBOOLA_BRANCH_ID=numeric-branch-id
KEBOOLA_SNOWFLAKE_DB=SAPI_xxxxx
KEBOOLA_SNOWFLAKE_SCHEMA=WORKSPACE_xxxxx
```

**DULEZITE:** `KEBOOLA_WORKSPACE_ID` musi byt ID z Storage API (`/v2/storage/workspaces`), ne cislo ze `SNOWFLAKE_USER`!

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

### dbt_project.yml (dispatch konfigurace)
Pro spravne pouziti Keboola maker pridat:
```yaml
dispatch:
  - macro_namespace: dbt
    search_order: ['keboola', 'dbt']
```

## Keboola Workspace vs Schema

- **Workspace schema** (napr. `WORKSPACE_1282429287`) = **zapisovatelne** - sem dbt zapisuje tabulky
- **Ostatni schema** (napr. `out.c-amplitude`) = **read-only** - zdrojova data

Priklad SQL dotazu na zdrojova data:
```sql
SELECT "platform", "event_type", count("event_id") as "events"
FROM "SAPI_10504"."out.c-amplitude"."events"
GROUP BY 1,2 ORDER BY 3 DESC;
```

## Klicove implementacni detaily

### Namespace packages
Soubory `dbt/__init__.py` a `dbt/adapters/__init__.py` MUSI obsahovat:
```python
__path__ = __import__('pkgutil').extend_path(__path__, __name__)
```
Jinak se lokalni package prekryje s instalovanym dbt.

### Case-insensitive matching
Snowflake uklada identifikatory v UPPERCASE. `KeboolaRelation` implementuje:
- `_is_exactish_match()` - case-insensitive porovnani relaci
- `matches()` - case-insensitive vyhledavani

### CREATE OR REPLACE
Makro `keboola__get_create_table_as_sql` pouziva `CREATE OR REPLACE TABLE` pro idempotentni operace.

### Transakce (simulovane)
REST API nepodporuje transakce, metody `begin()`, `commit()`, `rollback()` jsou no-op.

### KeboolaCredentials
- Dedi z `dbt.adapters.contracts.connection.Credentials`
- Base class ma `database` a `schema` jako required fields
- Vsechny nase fields musi mit defaults (jinak dataclass error)

### KeboolaCursor
- Emuluje DB-API 2.0 cursor nad REST API
- `execute()` vola `client.execute_query()` z keboola-query-service SDK
- Vysledky uklada do pameti, `fetchall()` je vraci

## Keboola Query Service API

### Potrebne credentials
- **token**: Keboola Storage API token (zacina project_id-)
- **workspace_id**: CISELNE ID workspace z `/v2/storage/workspaces` API
- **branch_id**: CISELNE ID branch (ne "default")

### SDK usage
```python
from keboola_query_service import Client

client = Client(base_url="https://query.keboola.com", token=token)
results = client.execute_query(
    branch_id="1261313",
    workspace_id="2950196630",  # ID z Storage API!
    statements=["SELECT 1"],
    transactional=True,
)
```

### Jak zjistit spravne Workspace ID
```python
import httpx
headers = {'X-StorageApi-Token': token}
resp = httpx.get('https://connection.keboola.com/v2/storage/workspaces', headers=headers)
for ws in resp.json():
    print(f"ID: {ws['id']} - {ws['name']}")
```

## Troubleshooting

### "No module named 'dbt.adapters.keboola'"
- Pouzij non-editable install: `pip install .` (ne `pip install -e .`)
- Over namespace package __init__.py soubory

### "Failed to parse branch ID"
- branch_id musi byt ciselne, ne "default"
- Najdes v Keboola UI: Settings > Branches

### "Failed to get workspace credentials (403)"
- Over ze workspace_id je z `/v2/storage/workspaces` API
- Over ze token ma pristup k workspace
- Over ze workspace je na spravne branch

### "approximate match" / case sensitivity error
- Snowflake je case-insensitive, ale dbt hleda case-sensitive
- `KeboolaRelation` musi implementovat `_is_exactish_match()` a `matches()`

### Python 3.14 errors
- dbt neni kompatibilni s Python 3.14
- Pouzij Python 3.13 nebo 3.11

## Status

- [x] Zakladni adapter funguje
- [x] `dbt debug` projde
- [x] `dbt run` s table materializaci funguje
- [x] Idempotentni operace (opakovane spusteni)
- [x] Cteni z read-only schemat (out.c-amplitude)
- [ ] View materializace
- [ ] Incremental materializace
- [ ] Snapshot materializace
- [ ] Unit testy
