-- Our accounts whose email hash appears in a third-party breach dump.
with accounts as (
    select
        user_id,
        any_value(username)        as username,
        any_value(username_sha256) as username_sha256
    from {{ ref('stg_auth_events') }}
    where user_id is not null
    group by 1
)

select
    a.user_id,
    a.username,
    count(distinct b.breach_source)  as breach_count,
    min(b.breach_date)               as first_breach_date,
    min(b.received_date)             as first_seen_in_feed,
    string_agg(distinct b.breach_source, ', ' order by b.breach_source) as breach_sources
from accounts as a
join {{ ref('stg_breach_records') }} as b on b.email_sha256 = a.username_sha256
group by 1, 2
