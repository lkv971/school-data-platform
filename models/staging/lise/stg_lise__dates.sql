select
    Date,
    CalendarYear,
    CalendarMonth,
    MonthName,
    SchoolYear
from {{ source('lise', 'Dates') }}