-- One row per user per day with an explainable 0-100 risk score.
--   +40 impossible travel        +30 attacker got in during an incident
--   +15 targeted by any incident +10 credentials exposed in a breach
--   +1 per failed login (max +5)
with activity as (
    select
        user_id,
        event_date,
        any_value(username)                                as username,
        count(*)                                           as login_attempts,
        sum(case when is_failure then 1 else 0 end)        as failed_logins,
        count(distinct src_ip)                             as distinct_ips,
        count(distinct geo_country)                        as distinct_countries
    from {{ ref('stg_auth_events') }}
    where user_id is not null
    group by 1, 2
),

incident_hits as (
    select
        user_id,
        event_date,
        count(distinct incident_id)       as incidents,
        bool_or(attacker_succeeded)       as compromised_in_incident
    from {{ ref('int_incident_accounts') }}
    where user_id is not null
    group by 1, 2
),

travel as (
    select user_id, cast(to_time as date) as event_date, count(*) as impossible_travel_events
    from {{ ref('fct_impossible_travel') }}
    group by 1, 2
),

scored as (
    select
        a.*,
        coalesce(i.incidents, 0)                     as incidents,
        coalesce(i.compromised_in_incident, false)   as compromised_in_incident,
        coalesce(t.impossible_travel_events, 0)      as impossible_travel_events,
        x.user_id is not null                        as credentials_exposed
    from activity as a
    left join incident_hits as i using (user_id, event_date)
    left join travel        as t using (user_id, event_date)
    left join {{ ref('dim_exposed_accounts') }} as x using (user_id)
)

select
    *,
    least(100,
          case when impossible_travel_events > 0 then 40 else 0 end
        + case when compromised_in_incident then 30 else 0 end
        + case when incidents > 0 then 15 else 0 end
        + case when credentials_exposed then 10 else 0 end
        + least(failed_logins, 5)
    ) as risk_score
from scored
