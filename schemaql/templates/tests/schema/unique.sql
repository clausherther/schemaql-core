with validation_errors as (
    
    select
        {{ column }}
    from {{ schema }}.{{ entity }}
    where {{ column }} is not null
    group by 
        {{ column }}
    having count(*) > 1
)
select count(*) as test_result
from
    validation_errors