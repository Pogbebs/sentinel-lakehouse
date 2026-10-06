-- Silver auth events, typed and renamed for analytics. One row per unique event_id.
-- Load-test traffic (sentinel.loadtest) is excluded so it never moves a metric.
select
    event_id,
    cast(event_time as timestamp)        as event_time,
    cast(event_date as date)             as event_date,
    user_id,
    username,
    username_sha256,
    src_ip,
    geo_country,
    geo_city,
    geo_lat,
    geo_lon,
    device_id,
    user_agent,
    app,
    auth_method,
    outcome,
    failure_reason,
    is_failure,
    label_attack_type,
    label_attack_id,
    cast(ingested_at as timestamp)       as ingested_at
from {{ lake_table('silver/auth_events') }}
where username not like '%@loadtest.invalid'
