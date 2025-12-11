{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='merge'
    )
}}

SELECT
    "event_id",
    "event_type",
    "user_id",
    "event_time",
    "event_properties",
    CURRENT_TIMESTAMP() as "loaded_at"
FROM {{ source('amplitude', 'events') }}

{% if is_incremental() %}
    -- Only process new events since last run
    WHERE "event_time" > (SELECT MAX("event_time") FROM {{ this }})
{% endif %}
