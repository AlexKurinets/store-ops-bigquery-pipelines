{#- Generic tests written here rather than pulled from a package, so `dbt build` needs no network access. -#}

{% test unique_combination_of(model, columns) %}
select {{ columns | join(', ') }}, count(*) as n
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}


{% test at_least(model, column_name, min_value) %}
select {{ column_name }} from {{ model }} where {{ column_name }} < {{ min_value }}
{% endtest %}
