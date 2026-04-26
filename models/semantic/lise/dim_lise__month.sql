with dates as (
    select * from {{ ref('stg_lise__dates') }}
)

select
    (CalendarYear * 100 + CalendarMonth) as MonthKey,
    cast(min([Date]) as date) as MonthStartDate,
    CalendarYear,
    CalendarMonth,
    MonthName,
    concat(left(MonthName, 3), ' ', cast(CalendarYear as varchar(4))) as MonthYearLabel,
    SchoolYear
from dates
group by
    CalendarYear,
    CalendarMonth,
    MonthName,
    SchoolYear