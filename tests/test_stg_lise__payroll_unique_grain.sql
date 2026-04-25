select
    PersonnelID,
    Periode,
    [Date],
    ProfessionTypeID,
    EtablissementID,
    ClasseID,
    NiveauID,
    count(*) as row_count
from {{ ref('stg_lise__payroll') }}
group by
    PersonnelID,
    Periode,
    [Date],
    ProfessionTypeID,
    EtablissementID,
    ClasseID,
    NiveauID
having count(*) > 1