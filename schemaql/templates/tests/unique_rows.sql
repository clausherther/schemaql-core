{% macro row_key(columns) %}
    concat(
    {% for col in columns -%}
        cast({{ col }} as {{ type_string() }}){% if not loop.last %},{% endif %}
    {% endfor -%}
    )
{% endmacro %}

{% set columns = kwargs["columns"] %}

with hashed_rows as (
    select 
        {{ hash(row_key(columns)) }} as row_hash
     from {{ schema }}.{{ table }}
)
select (count(*) - count(distinct row_hash)) as test_result
from hashed_rows