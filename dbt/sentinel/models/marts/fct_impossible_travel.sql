-- Two successful logins for one user that would need faster-than-a-jet travel.
-- Batch-only detector: it needs per-user history, which is cheap in SQL and expensive
-- to hold as streaming state for every user.
with legs as (
    select
        *,
        {{ haversine_km('prev_lat', 'prev_lon', 'geo_lat', 'geo_lon') }} as distance_km,
        epoch(event_time - prev_event_time) / 3600.0                   as hours_between
    from {{ ref('int_successful_logins_sequenced') }}
    where prev_event_time is not null
)

select
    md5('impossible_travel|' || event_id)            as incident_id,
    user_id,
    username,
    prev_event_time                                  as from_time,
    event_time                                       as to_time,
    prev_city || ', ' || prev_country                as from_location,
    geo_city || ', ' || geo_country                  as to_location,
    round(distance_km, 0)                            as distance_km,
    round(hours_between * 60, 1)                     as minutes_between,
    round(distance_km / greatest(hours_between, 1.0 / 60), 0) as implied_speed_kmh,
    device_id <> prev_device_id                      as new_device,
    event_id                                         as triggering_event_id,
    -- Either leg may be attacker activity. A takeover from another campaign (e.g. a brute
    -- force that succeeded from abroad) is a genuine catch, so it is credited to that attack.
    coalesce(label_attack_id, prev_label_attack_id)  as matched_attack_id,
    case when label_attack_id is not null then label_attack_type
         when prev_label_attack_id is not null then prev_label_attack_type end
                                                     as matched_attack_type
from legs
where distance_km >= {{ var('travel_min_distance_km') }}
  and distance_km / greatest(hours_between, 1.0 / 60) >= {{ var('travel_min_speed_kmh') }}
