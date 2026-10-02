-- Pipeline health: how much arrived, how much was rejected, and why. Both sides are keyed
-- on the day the record was ingested (a rejected event may have no usable event time).
with accepted as (
    select cast(ingested_at as date) as day, count(*) as accepted_events
    from {{ ref('stg_auth_events') }}
    group by 1
),

rejected as (
    select
        cast(ingested_at as date) as day,
        count(distinct (kafka_partition, kafka_offset)) as quarantined_events,
        count(*) filter (where dq_error = 'invalid_event_ts')  as invalid_event_ts,
        count(*) filter (where dq_error = 'invalid_outcome')   as invalid_outcome,
        count(*) filter (where dq_error = 'invalid_geo')       as invalid_geo,
        count(*) filter (where dq_error = 'missing_event_id')  as missing_event_id,
        count(*) filter (where dq_error not in
            ('invalid_event_ts', 'invalid_outcome', 'invalid_geo', 'missing_event_id'))
                                                               as other_errors
    from {{ ref('stg_quarantine') }}
    group by 1
)

select
    coalesce(a.day, r.day)                          as day,
    coalesce(a.accepted_events, 0)                  as accepted_events,
    coalesce(r.quarantined_events, 0)               as quarantined_events,
    round(coalesce(r.quarantined_events, 0)
          / nullif(coalesce(a.accepted_events, 0) + coalesce(r.quarantined_events, 0), 0), 5)
                                                    as quarantine_rate,
    coalesce(r.invalid_event_ts, 0)                 as invalid_event_ts,
    coalesce(r.invalid_outcome, 0)                  as invalid_outcome,
    coalesce(r.invalid_geo, 0)                      as invalid_geo,
    coalesce(r.missing_event_id, 0)                 as missing_event_id,
    coalesce(r.other_errors, 0)                     as other_errors
from accepted as a
full outer join rejected as r using (day)
order by day
