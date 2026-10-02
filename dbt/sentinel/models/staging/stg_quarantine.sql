-- Events rejected by the silver quality checks, one row per (event, failed rule).
select
    kafka_partition,
    kafka_offset,
    cast(ingested_at as timestamp)    as ingested_at,
    cast(quarantined_at as timestamp) as quarantined_at,
    cast(quarantine_date as date)     as quarantine_date,
    unnest(dq_errors)                 as dq_error,
    payload
from {{ lake_table('quarantine/auth_events') }}
