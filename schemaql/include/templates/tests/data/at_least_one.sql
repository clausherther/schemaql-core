with validation_errors as (
    select
      count(distinct {{ column }}) as cnt
    from {{ schema }}.{{ entity }}
    having count(distinct {{ column }}) = 0
)
select count(*) as test_result
from
    validation_errors