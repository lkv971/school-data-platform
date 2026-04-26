select
    CalendarYear,
    CalendarMonth,
    SchoolYear,
    EtablissementID,
    NiveauID,
    ClasseID,
    ProfessionTypeID,
    count(*) as row_count
from {{ ref('mart_lise__payroll_monthly') }}
group by
    CalendarYear,
    CalendarMonth,
    SchoolYear,
    EtablissementID,
    NiveauID,
    ClasseID,
    ProfessionTypeID
having count(*) > 1