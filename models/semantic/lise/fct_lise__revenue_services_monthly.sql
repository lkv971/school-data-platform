with revenue_services as (
    select * from {{ ref('mart_lise__revenue_services_monthly') }}
)

select
    (CalendarYear * 100 + CalendarMonth) as MonthKey,

    case
        when ClasseID is not null then concat('CLASS|', cast(ClasseID as varchar(50)))
        else 'UNKNOWN'
    end as ClassKey,

    coalesce(ServiceID, -1) as ServiceKey,

    ServiceLineCount,
    StudentCount,
    TotalQuantity,
    TotalServiceRevenue,
    AvgServiceRevenuePerLine
from revenue_services