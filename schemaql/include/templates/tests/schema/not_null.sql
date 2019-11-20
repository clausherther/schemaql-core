with validation_errors as (
    select
        *
    from 
        {{ schema }}.{{ entity }}
    where
        {{ column }} is null
)
select count(*) as test_result
from
    validation_errors