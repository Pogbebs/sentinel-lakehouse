-- Privacy guard: the breach feed must only contain 64-char SHA-256 hex digests.
select email_sha256 from {{ ref('stg_breach_records') }}
where not regexp_matches(email_sha256, '^[0-9a-f]{64}$')
