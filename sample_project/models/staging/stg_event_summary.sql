{{ config(
    materialized='table',
    description='Summary of events by type and user'
) }}

SELECT
    "user_id",
    "event_type",
    COUNT(*) as "event_count",
    MIN("event_time") as "first_event_time",
    MAX("event_time") as "last_event_time"
FROM {{ ref('stg_events') }}
GROUP BY "user_id", "event_type"
