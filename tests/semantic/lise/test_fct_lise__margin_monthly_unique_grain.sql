select
    MonthKey,
    ClassKey,
    count(*) as row_count
from {{ ref('fct_lise__margin_monthly') }}
group by
    MonthKey,
    ClassKey
having count(*) > 1