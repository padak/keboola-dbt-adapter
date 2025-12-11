-- Test model reading from Amplitude events
{{ config(materialized='table') }}

select
    "platform",
    "event_type",
    count("event_id") as "events"
from "SAPI_10504"."out.c-amplitude"."events"
group by 1, 2
order by 3 desc
limit 100
