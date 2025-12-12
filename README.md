# dbt-keboola

dbt adapter for Keboola Query Service - run dbt models through Keboola REST API instead of direct Snowflake connection.

## Quickstart

```bash
# 1. Clone
git clone https://github.com/keboola/dbt-keboola.git
cd dbt-keboola

# 2. Python venv (requires Python 3.13, NOT 3.14!)
python3.13 -m venv .venv
source .venv/bin/activate

# 3. Install
pip install .

# 4. Create .env
cat > .env << 'EOF'
KEBOOLA_API_TOKEN=your-token
KEBOOLA_WORKSPACE_ID=your-workspace-id
KEBOOLA_BRANCH_ID=your-branch-id
KEBOOLA_SNOWFLAKE_DB=SAPI_xxxxx
KEBOOLA_SNOWFLAKE_SCHEMA=WORKSPACE_xxxxx
EOF

# 5. Load env and run
set -a && source .env && set +a
dbt debug --project-dir sample_project --profiles-dir sample_project
dbt run --project-dir sample_project --profiles-dir sample_project --select test_simple
```

If `dbt debug` passes and `test_simple` model runs successfully, the adapter works.

## Configuration

### Environment Variables

| Variable | Description |
|----------|-------------|
| `KEBOOLA_API_TOKEN` | Keboola Storage API token |
| `KEBOOLA_WORKSPACE_ID` | Workspace ID from `/v2/storage/workspaces` API |
| `KEBOOLA_BRANCH_ID` | Numeric branch ID (not "default") |
| `KEBOOLA_SNOWFLAKE_DB` | Database name (e.g., `SAPI_10504`) |
| `KEBOOLA_SNOWFLAKE_SCHEMA` | Workspace schema (e.g., `WORKSPACE_1282429287`) |

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

## Status

- [x] Table materialization
- [ ] View materialization
- [ ] Incremental materialization
- [ ] Snapshot materialization

## License

MIT
