{{ config(materialized='view') }}

SELECT
    "event_id",
    "event_type",
    "user_id",
    "event_time",
    "event_properties"
FROM {{ source('amplitude', 'events') }}
LIMIT 1000
