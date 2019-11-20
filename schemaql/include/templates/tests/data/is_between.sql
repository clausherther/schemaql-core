{%- set min_value = kwargs["min_value"] -%}
{%- set max_value = kwargs["max_value"] -%}
with validation_errors as (
    select
        *
    from 
        {{ schema }}.{{ entity }}
    where
        not (
            {{ column }} between {{ min_value }} and {{ max_value }}
        )
)
select count(*) as test_result
from
    validation_errors