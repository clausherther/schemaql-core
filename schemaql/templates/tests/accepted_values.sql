select count(*) as test_result
from {{ table }}
where
    {{ column }} not in (
        {%- for val in kwargs["values"] -%}
        '{{ val }}'{% if not loop.last %},{% endif %}
        {% endfor -%}
        )