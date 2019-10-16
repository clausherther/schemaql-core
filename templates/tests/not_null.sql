select count(*) as test_result
from {{ schema }}.{{ table }}
where
    {{ column }} is null