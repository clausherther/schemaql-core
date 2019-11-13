select count(distinct {{ column }}) as metric_result
from {{ schema }}.{{ entity }}