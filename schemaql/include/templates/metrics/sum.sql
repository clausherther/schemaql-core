select sum({{ column }}) as metric_result
from {{ schema }}.{{ entity }}