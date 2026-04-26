with revenue as (
    select * from {{ ref('mart_lise__revenue_monthly') }}
)

select
    (CalendarYear * 100 + CalendarMonth) as MonthKey,

    case
        when ClasseID is not null then concat('CLASS|', cast(ClasseID as varchar(50)))
        else 'UNKNOWN'
    end as ClassKey,

    coalesce(RegimeID, -1) as RegimeKey,

    RevenueLineCount,
    StudentCount,
    TotalBilledRevenue,
    AvgBilledPerLine
from revenue