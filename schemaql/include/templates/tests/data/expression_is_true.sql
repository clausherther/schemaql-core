{% set expression = kwargs.get('expression', kwargs.get('arg')) %}
{% set condition = kwargs.get('condition', '1=1') %}

with rows_with_condition as (

    select * 
    from 
        {{ schema }}.{{ entity }}
    where 
        {{ condition }}

),
validation_errors as (

    select
        *
    from 
        rows_with_condition
    where 
        not({{ expression }})

)

select count(*) as test_result
from validation_errors