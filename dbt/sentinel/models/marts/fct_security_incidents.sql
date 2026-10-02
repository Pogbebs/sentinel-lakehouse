-- One row per security incident, from both detection paths, scored against ground truth.
with stream_matches as (
    select
        incident_id,
        -- The attack this incident most plausibly corresponds to (same type, most events).
        arg_max(label_attack_id, n)   as matched_attack_id,
        arg_max(label_attack_type, n) as matched_attack_type
    from (
        select incident_id, label_attack_id, label_attack_type, count(*) as n
        from {{ ref('int_incident_accounts') }}
        where label_attack_type = rule
        group by 1, 2, 3
    )
    group by 1
),

stream as (
    select
        i.incident_id,
        i.rule,
        'streaming'               as detection_path,
        i.entity_type,
        i.entity_value,
        i.incident_start,
        i.incident_end,
        i.first_alert_window_end  as earliest_detectable_at,
        i.alert_count,
        i.severity,
        i.peak_distinct_users     as accounts_targeted,
        m.matched_attack_id,
        m.matched_attack_type
    from {{ ref('int_alert_incidents') }} as i
    left join stream_matches as m using (incident_id)
),

travel as (
    select
        incident_id,
        'impossible_travel'       as rule,
        'batch'                   as detection_path,
        'account'                 as entity_type,
        username                  as entity_value,
        from_time                 as incident_start,
        to_time                   as incident_end,
        to_time                   as earliest_detectable_at,
        1                         as alert_count,
        case when new_device then 'critical' else 'high' end as severity,
        1                         as accounts_targeted,
        matched_attack_id,
        matched_attack_type
    from {{ ref('fct_impossible_travel') }}
)

select *, matched_attack_id is not null as is_true_positive from stream
union all
select *, matched_attack_id is not null as is_true_positive from travel
