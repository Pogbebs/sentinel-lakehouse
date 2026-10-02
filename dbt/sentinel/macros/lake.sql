{#
  Read a lake table regardless of storage format.
  - delta:   delta_scan() honours the transaction log (no stale or uncommitted files).
  - parquet: hive-partitioned Parquet written by the offline replay job.
#}
{% macro lake_table(relative_path) -%}
  {%- set root = var('lake_root') | replace('s3a://', 's3://') -%}
  {%- if var('lake_format') == 'delta' -%}
    delta_scan('{{ root }}/{{ relative_path }}')
  {%- else -%}
    read_parquet('{{ root }}/{{ relative_path }}/**/*.parquet', hive_partitioning = true, union_by_name = true)
  {%- endif -%}
{%- endmacro %}

{% macro landing_csv(relative_glob) -%}
  {%- set root = var('lake_root') | replace('s3a://', 's3://') -%}
  read_csv('{{ root }}/landing/{{ relative_glob }}', header = true, hive_partitioning = true, all_varchar = true)
{%- endmacro %}

{# Great-circle distance in km. #}
{% macro haversine_km(lat1, lon1, lat2, lon2) -%}
  2 * 6371 * asin(sqrt(
      power(sin(radians({{ lat2 }} - {{ lat1 }}) / 2), 2)
    + cos(radians({{ lat1 }})) * cos(radians({{ lat2 }}))
      * power(sin(radians({{ lon2 }} - {{ lon1 }}) / 2), 2)
  ))
{%- endmacro %}
