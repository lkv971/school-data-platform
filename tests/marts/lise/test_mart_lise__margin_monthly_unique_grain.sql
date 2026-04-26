select
    CalendarYear,
    CalendarMonth,
    SchoolYear,
    EtablissementID,
    NiveauID,
    ClasseID,
    count(*) as row_count
from {{ ref('mart_lise__margin_monthly') }}
group by
    CalendarYear,
    CalendarMonth,
    SchoolYear,
    EtablissementID,
    NiveauID,
    ClasseID
having count(*) > 1