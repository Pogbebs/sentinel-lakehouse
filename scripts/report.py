"""Print the headline numbers from the gold layer after `make demo`."""

import os

import duckdb

con = duckdb.connect(
    os.environ.get("DUCKDB_PATH", "data/warehouse/sentinel.duckdb"), read_only=True
)
print("\nDetection quality (scored against simulator ground truth)")
con.sql("""
    select rule, attacks_total as attacks, attacks_detected as detected, incidents,
           false_positive_incidents as false_pos, precision, recall,
           median_minutes_to_detect as median_min_to_detect
    from mart_detection_quality order by rule
""").show()
print("Highest-risk accounts")
con.sql("""
    select username, event_date, risk_score, incidents, compromised_in_incident as compromised,
           impossible_travel_events as travel, credentials_exposed as exposed
    from mart_user_risk_daily order by risk_score desc, username limit 10
""").show()
print("Data quality")
con.sql("select * from mart_data_quality_daily").show()
