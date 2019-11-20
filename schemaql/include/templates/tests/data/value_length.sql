with validation_errors as (
    select
        *
    from {{ schema }}.{{ entity }}
    where
        {% if "min_value" in kwargs -%}
        {%- set min_value = kwargs["min_value"] -%}
        length(trim({{ column }})) >= {{ min_value }} and
        {%- endif %}
        {% if "max_value" in kwargs -%}
        {%- set max_value = kwargs["max_value"] -%}
        length(trim({{ column }})) <= {{ max_value }} and
        {%- endif %}
        1=1
)
select count(*) as test_result
from
    validation_errors