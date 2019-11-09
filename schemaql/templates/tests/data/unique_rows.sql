{%- if kwargs and "columns" in kwargs %}
{%- set columns = kwargs["columns"] -%}
{%- else -%}
{%- set columns = connector.get_column_names(entity, schema) -%}
{%- endif -%}
{%- if kwargs and "except" in kwargs %}
{%- set except_columns = kwargs["except"] -%}
{%- set columns = columns | difference(except_columns) -%}
{%- endif -%}
with hashed_rows as (
    select 
        {{ hash(columns) }} as row_hash
     from {{ schema }}.{{ entity }}
)
select (count(*) - count(distinct row_hash)) as test_result
from hashed_rows