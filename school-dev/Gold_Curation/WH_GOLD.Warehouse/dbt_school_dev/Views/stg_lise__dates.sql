-- Auto Generated (Do not modify) B49D6A43ECA2C911785BB326927E819371D7495BCB8E01D91E9461D8C70CC76D
create view "dbt_school_dev"."stg_lise__dates" as select
    [Date],
    CalendarYear,
    CalendarMonth,
    MonthName,
    SchoolYear
from "WH_GOLD"."LISE"."Dates";