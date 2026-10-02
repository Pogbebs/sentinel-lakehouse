select * from {{ ref('mart_user_risk_daily') }}
where risk_score < 0 or risk_score > 100
