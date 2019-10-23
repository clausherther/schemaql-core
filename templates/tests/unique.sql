select (count(*) - count(distinct {{ column }}))as test_result
from {{ table }}