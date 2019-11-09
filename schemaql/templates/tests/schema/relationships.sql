{% set other_entity = kwargs["to"] %}
{% if not '.' in other_entity %}
{% set other_entity = schema ~ '.' ~ other_entity  %}
{% endif %}
{% set other_column = kwargs["field"] %}
select count(*) as test_result
from
    {{ schema }}.{{ entity }} c
    left outer join
    {{ other_entity }} p on c.{{ column }} = p.{{ other_column }}
where
    p.{{ other_column }} is null