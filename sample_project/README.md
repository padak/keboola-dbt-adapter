# Keboola dbt Sample Project

This is a sample dbt project demonstrating how to use the dbt-keboola adapter.

## Setup

1. Set the required environment variables:

```bash
export KBC_TOKEN="your-keboola-token"
export KBC_WORKSPACE_ID="your-workspace-id"
export KBC_BRANCH_ID="default"  # optional, defaults to 'default'
```

2. Install dbt-keboola:

```bash
pip install dbt-keboola
```

3. Test the connection:

```bash
dbt debug --profiles-dir .
```

## Project Structure

- `models/staging/` - Staging models that select from source tables
  - `stg_events.sql` - Example staging model selecting from Amplitude events
  - `schema.yml` - Source and model definitions

## Running the Project

1. Run all models:

```bash
dbt run --profiles-dir .
```

2. Test models:

```bash
dbt test --profiles-dir .
```

3. Generate documentation:

```bash
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .
```

## Customizing for Your Data

1. Update `models/staging/schema.yml` to reference your actual Keboola Storage tables
2. Modify `stg_events.sql` to match your table structure
3. Add additional models in the `models/` directory
4. Configure materializations in `dbt_project.yml`

## Available Materializations

- `view` - Creates a view (default)
- `table` - Creates a table
- `incremental` - Creates an incremental table with merge, delete+insert, or append strategies

Example incremental model:

```sql
{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='merge'
) }}

SELECT * FROM {{ source('amplitude', 'events') }}
{% if is_incremental() %}
WHERE "event_time" > (SELECT MAX("event_time") FROM {{ this }})
{% endif %}
```
