select count(*) as test_result
from {{ schema }}.{{ entity }}
where
    trim({{ column }}) = ''