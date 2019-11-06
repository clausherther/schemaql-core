{% set other_table = kwargs["to"] %}
{% if not '.' in other_table %}
{% set other_table = schema ~ '.' ~ other_table  %}
{% endif %}
{% set other_column = kwargs["field"] %}
select count(*) as test_result
from
    {{ schema }}.{{ table }} c
    left outer join
    {{ other_table }} p on c.{{ column }} = p.{{ other_column }}
where
    p.{{ other_column }} is null