-- Auto Generated (Do not modify) A7CB90B77612B197B80C74724FD111E34A1092BE676C8F9815C9C2BE864168EB
create view "dbt_school_dev"."mart_lise__payroll_monthly" as with payroll as (
    select * from "WH_GOLD"."dbt_school_dev"."stg_lise__payroll"
),
dates as (
    select * from "WH_GOLD"."dbt_school_dev"."stg_lise__dates"
),
prof_types as (
    select * from "WH_GOLD"."dbt_school_dev"."stg_lise__profession_types"
),
classes as (
    select * from "WH_GOLD"."dbt_school_dev"."stg_lise__classes"
),
niveaux as (
    select * from "WH_GOLD"."dbt_school_dev"."stg_lise__niveaux"
),
etabs as (
    select * from "WH_GOLD"."dbt_school_dev"."stg_lise__etablissements"
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
    count(*) as payroll_line_count,
    count(distinct p.PersonnelID) as personnel_count,
    sum(p.payroll_amount) as total_payroll_amount,
    avg(p.payroll_amount) as avg_payroll_amount
from payroll p
left join dates d
    on p.payroll_date = d.[Date]
left join prof_types pt
    on p.ProfessionTypeID = pt.ProfessionTypeID
left join classes c
    on p.ClasseID = c.ClasseID
left join niveaux n
    on coalesce(p.NiveauID, c.NiveauID) = n.NiveauID
left join etabs e
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
    pt.ProfessionType;