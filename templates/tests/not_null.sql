select count(*) as test_result
from {{ table }}
where
    {{ column }} is null