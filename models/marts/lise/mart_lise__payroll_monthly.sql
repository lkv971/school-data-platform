with payroll as (
    select * from {{ ref('stg_lise__payroll') }}
),
dates as (
    select * from {{ ref('stg_lise__dates') }}
),
profession_types as (
    select * from {{ ref('stg_lise__profession_types') }}
),
classes as (
    select * from {{ ref('stg_lise__classes') }}
),
niveaux as (
    select * from {{ ref('stg_lise__niveaux') }}
),
etablissements as (
    select * from {{ ref('stg_lise__etablissements') }}
)

select
    d.CalendarYear,
    d.CalendarMonth,
    d.MonthName,
    d.SchoolYear,
    p.EtablissementID,
    e.Etablissement,
    coalesce(p.NiveauID, c.NiveauID) as NiveauID,
    n.Niveau,
    p.ClasseID,
    c.Classe,
    c.ClasseLibelle,
    p.ProfessionTypeID,
    pt.ProfessionType,
    count(*) as PayrollLineCount,
    count(distinct p.PersonnelID) as PersonnelCount,
    sum(p.Montant) as TotalPayrollAmount,
    avg(p.Montant) as AvgPayrollAmount
from payroll p
left join dates d
    on p.Date = d.Date
left join profession_types pt
    on p.ProfessionTypeID = pt.ProfessionTypeID
left join classes c
    on p.ClasseID = c.ClasseID
left join niveaux n
    on coalesce(p.NiveauID, c.NiveauID) = n.NiveauID
left join etablissements e
    on p.EtablissementID = e.EtablissementID
group by
    d.CalendarYear,
    d.CalendarMonth,
    d.MonthName,
    d.SchoolYear,
    p.EtablissementID,
    e.Etablissement,
    coalesce(p.NiveauID, c.NiveauID),
    n.Niveau,
    p.ClasseID,
    c.Classe,
    c.ClasseLibelle,
    p.ProfessionTypeID,
    pt.ProfessionType