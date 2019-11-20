with validation_errors as (
    select
        *
    from {{ schema }}.{{ entity }}
    where
        trim({{ column }}) = ''
)
select count(*) as test_result
from
    validation_errors