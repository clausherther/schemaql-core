{%- set values = kwargs["values"] if "values" in kwargs else [] -%}
{%- set quote_values = kwargs["quote"] if "quote" in kwargs else True -%}
with all_values as (
    select distinct
        {{ column }} as value_field
    from {{ schema }}.{{ entity }}
    {%- if 'null' in values or 'NULL' in values %}
    where {{ column }} is not null
    {% endif -%}
),
validation_errors as (

    select
        value_field

    from all_values
    where 
        value_field in (
        {% for val in values -%}
            {% if val|lower != 'null' %}
                {% if quote_values -%}
                '{{ val }}'
                {%- else -%}
                {{ val }}
                {%- endif -%}
                {%- if not loop.last -%},{%- endif %}
            {%- endif -%}
        {%- endfor %}
    )
)

select count(*) as test_result
from validation_errors