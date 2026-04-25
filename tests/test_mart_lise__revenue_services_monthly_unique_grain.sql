select
    CalendarYear,
    CalendarMonth,
    SchoolYear,
    EtablissementID,
    NiveauID,
    ClasseID,
    ServiceID,
    count(*) as row_count
from {{ ref('mart_lise__revenue_services_monthly') }}
group by
    CalendarYear,
    CalendarMonth,
    SchoolYear,
    EtablissementID,
    NiveauID,
    ClasseID,
    ServiceID
having count(*) > 1