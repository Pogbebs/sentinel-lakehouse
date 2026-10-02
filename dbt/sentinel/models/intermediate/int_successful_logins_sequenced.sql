-- Each successful login next to the same user's previous successful login.
select
    event_id,
    user_id,
    username,
    event_time,
    geo_country,
    geo_city,
    geo_lat,
    geo_lon,
    src_ip,
    device_id,
    label_attack_type,
    label_attack_id,
    lag(event_time)  over w as prev_event_time,
    lag(geo_country) over w as prev_country,
    lag(geo_city)    over w as prev_city,
    lag(geo_lat)     over w as prev_lat,
    lag(geo_lon)     over w as prev_lon,
    lag(device_id)   over w as prev_device_id,
    lag(label_attack_type) over w as prev_label_attack_type,
    lag(label_attack_id)   over w as prev_label_attack_id
from {{ ref('stg_auth_events') }}
where outcome = 'success'
  and user_id is not null
window w as (partition by user_id order by event_time, event_id)
