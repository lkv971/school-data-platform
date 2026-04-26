select
    MonthKey,
    ClassKey,
    ServiceKey,
    count(*) as row_count
from {{ ref('fct_lise__revenue_services_monthly') }}
group by
    MonthKey,
    ClassKey,
    ServiceKey
having count(*) > 1