select
    MonthKey,
    ClassKey,
    RegimeKey,
    count(*) as row_count
from {{ ref('fct_lise__revenue_monthly') }}
group by
    MonthKey,
    ClassKey,
    RegimeKey
having count(*) > 1