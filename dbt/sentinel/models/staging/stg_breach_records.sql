-- Third-party breach dumps dropped in the landing zone. Only hashes are ever stored.
-- A credential can appear in several dumps; keep the first time we learned about it.
with raw as (
    select
        lower(email_sha256)          as email_sha256,
        lower(password_sha1)         as password_sha1,
        source                       as breach_source,
        try_cast(breach_date as date) as breach_date,
        cast(dt as date)             as received_date
    from {{ landing_csv('breach_dumps/*/dump.csv') }}
    where email_sha256 is not null
)
select *
from raw
qualify row_number() over (
    partition by email_sha256, breach_source order by received_date
) = 1
