{# A tiny local stand-in for dbt_utils.unique_combination_of_columns (no package install needed). #}
{% test dbt_utils_unique_combination(model, columns) %}
select {{ columns | join(', ') }}, count(*) as n
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}
