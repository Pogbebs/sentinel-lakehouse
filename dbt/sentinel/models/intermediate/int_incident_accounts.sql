-- Every account touched by a streaming incident, with whether the attacker got in.
select
    i.incident_id,
    i.rule,
    e.user_id,
    e.username,
    e.event_date,
    e.label_attack_type,
    e.label_attack_id,
    bool_or(e.outcome = 'success') as attacker_succeeded
from {{ ref('int_alert_incidents') }} as i
join {{ ref('stg_auth_events') }} as e
  on e.event_time >= i.incident_start
 and e.event_time <  i.incident_end
 and (
        (i.entity_type = 'account'   and e.username = i.entity_value)
     or (i.entity_type = 'source_ip' and e.src_ip   = i.entity_value)
 )
group by all
