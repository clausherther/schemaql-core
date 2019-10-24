select count(*) as test_result
from
    {# {{ database }}.{{ schema }}. #}
    {{ table }} c
    left outer join
    {# {{ database }}.{{ schema }}. #}
    {{ kwargs["to"] }} p
        on c.{{ column }} = p.{{ kwargs["field"] }}
where
    p.{{ kwargs["field"] }} is null