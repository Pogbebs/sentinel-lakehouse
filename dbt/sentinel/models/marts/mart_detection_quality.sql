-- Precision, recall and time-to-detect per rule. This is what a detection engineer tunes
-- thresholds against, and what tests/assert_detection_quality.sql gates on.
with incidents as (
    select
        rule,
        count(*)                                        as incidents,
        sum(case when is_true_positive then 1 else 0 end) as true_positive_incidents,
        -- Recall only counts attacks of the rule's own type; precision credits any real attack.
        count(distinct case when matched_attack_type = rule then matched_attack_id end)
                                                        as attacks_detected
    from {{ ref('fct_security_incidents') }}
    group by 1
),

truth as (
    select attack_type as rule, count(*) as attacks_total
    from {{ ref('int_attack_ground_truth') }}
    group by 1
),

latency as (
    select
        i.rule,
        median(epoch(i.first_detectable - g.attack_start) / 60.0) as median_minutes_to_detect
    from (
        select rule, matched_attack_id, min(earliest_detectable_at) as first_detectable
        from {{ ref('fct_security_incidents') }}
        where matched_attack_type = rule
        group by 1, 2
    ) as i
    join {{ ref('int_attack_ground_truth') }} as g on g.attack_id = i.matched_attack_id
    group by 1
)

select
    t.rule,
    t.attacks_total,
    coalesce(i.attacks_detected, 0)                                     as attacks_detected,
    coalesce(i.incidents, 0)                                            as incidents,
    coalesce(i.true_positive_incidents, 0)                              as true_positive_incidents,
    coalesce(i.incidents, 0) - coalesce(i.true_positive_incidents, 0)   as false_positive_incidents,
    round(i.true_positive_incidents / nullif(i.incidents, 0), 3)        as precision,
    round(coalesce(i.attacks_detected, 0) / nullif(t.attacks_total, 0), 3) as recall,
    round(l.median_minutes_to_detect, 1)                                as median_minutes_to_detect
from truth as t
left join incidents as i using (rule)
left join latency   as l using (rule)
order by t.rule
