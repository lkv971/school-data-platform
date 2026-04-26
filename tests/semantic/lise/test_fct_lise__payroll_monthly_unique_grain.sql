select
    MonthKey,
    ClassKey,
    ProfessionTypeKey,
    count(*) as row_count
from {{ ref('fct_lise__payroll_monthly') }}
group by
    MonthKey,
    ClassKey,
    ProfessionTypeKey
having count(*) > 1