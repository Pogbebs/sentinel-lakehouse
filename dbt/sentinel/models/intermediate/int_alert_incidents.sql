-- Sliding windows make one attack raise several overlapping alerts. Collapse each run of
-- overlapping/adjacent windows for the same (rule, entity) into one incident
-- (classic gaps-and-islands).
with ordered as (
    select
        *,
        max(window_end) over (
            partition by rule, entity_value
            order by window_start
            rows between unbounded preceding and 1 preceding
        ) as running_prev_end
    from {{ ref('stg_streaming_alerts') }}
),

flagged as (
    select
        *,
        case when running_prev_end is null or window_start > running_prev_end then 1 else 0 end
            as starts_new_incident
    from ordered
),

grouped as (
    select
        *,
        sum(starts_new_incident) over (
            partition by rule, entity_value
            order by window_start
            rows between unbounded preceding and current row
        ) as incident_seq
    from flagged
)

select
    md5(rule || '|' || entity_value || '|' || cast(min(window_start) as varchar)) as incident_id,
    rule,
    any_value(entity_type)                         as entity_type,
    entity_value,
    min(window_start)                              as incident_start,
    max(window_end)                                as incident_end,
    min(window_end)                                as first_alert_window_end,
    count(*)                                       as alert_count,
    max(attempts)                                  as peak_attempts_per_window,
    max(distinct_users)                            as peak_distinct_users,
    max(successes)                                 as peak_successes,
    case when bool_or(severity = 'critical') then 'critical' else 'high' end as severity
from grouped
group by rule, entity_value, incident_seq
