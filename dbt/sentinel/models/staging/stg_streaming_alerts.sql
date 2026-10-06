-- Alerts emitted by the Spark streaming detectors (or the batch replay of them).
select
    alert_id,
    rule,
    severity,
    entity_type,
    entity_value,
    cast(window_start as timestamp) as window_start,
    cast(window_end as timestamp)   as window_end,
    attempts,
    failures,
    successes,
    distinct_users,
    unknown_user_failures,
    cast(detected_at as timestamp)  as detected_at
from {{ lake_table('gold/streaming_alerts') }}
where entity_value not like '%@loadtest.invalid'  -- load-test probes (sentinel.loadtest)
qualify row_number() over (partition by alert_id order by detected_at) = 1
