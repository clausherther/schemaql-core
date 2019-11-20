select count(distinct {{ column }})
from {{ schema }}.{{ entity }}
{%- if kwargs and "filter_condition" in kwargs %}
where
    {{ kwargs["filter_condition"] }}
{%- endif %}
