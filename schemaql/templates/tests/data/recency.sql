{% set datepart = kwargs.get('datepart', 'd') %}
{% set interval = kwargs.get('interval', 1) %}
with recent_data as (
    select
        count(*) as cnt
    from 
        {{ schema }}.{{ entity }}
    where 
        {{ column }} >=
        {{ dateadd(datepart, interval * -1, current_timestamp()) }}
)
select
    case 
        when cnt > 0 then 0
        else 1
    end as test_result
from 
    recent_data
