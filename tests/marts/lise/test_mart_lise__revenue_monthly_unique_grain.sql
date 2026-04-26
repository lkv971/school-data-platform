select
    CalendarYear,
    CalendarMonth,
    SchoolYear,
    EtablissementID,
    NiveauID,
    ClasseID,
    RegimeID,
    count(*) as row_count
from {{ ref('mart_lise__revenue_monthly') }}
group by
    CalendarYear,
    CalendarMonth,
    SchoolYear,
    EtablissementID,
    NiveauID,
    ClasseID,
    RegimeID
having count(*) > 1