{%- set values = kwargs["values"] if "values" in kwargs else [] -%}
{%- set quote_values = kwargs["quote"] if "quote" in kwargs else True -%}

with all_values as (
    select distinct
        {{ column }} as value_field
    from {{ schema }}.{{ entity }}
),
validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        {% for val in values -%}
            {% if quote_values -%}
            '{{ val }}'
            {%- else -%}
            {{ val }}
            {%- endif -%}
            {%- if not loop.last -%},{%- endif %}
        {%- endfor %}
    )
)

select count(*) as test_result
from validation_errors
