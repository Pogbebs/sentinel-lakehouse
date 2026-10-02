-- What actually happened, from the simulator's labels. Used only for scoring detections.
select
    label_attack_type                         as attack_type,
    label_attack_id                           as attack_id,
    min(event_time)                           as attack_start,
    max(event_time)                           as attack_end,
    count(*)                                  as attack_events,
    count(distinct username)                  as targeted_accounts,
    sum(case when outcome = 'success' then 1 else 0 end) as successful_attempts
from {{ ref('stg_auth_events') }}
where label_attack_id is not null
group by 1, 2
