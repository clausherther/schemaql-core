with validation_errors as (
    select
      count({{ column }})
    from {{ schema }}.{{ table }}
    having count({{ column }}) = 0
)
select count(*) as test_result
from
    validation_errors