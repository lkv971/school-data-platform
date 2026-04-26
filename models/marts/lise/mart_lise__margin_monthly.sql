with payroll as (
    select * from {{ ref('mart_lise__payroll_monthly') }}
),
revenue as (
    select * from {{ ref('mart_lise__revenue_monthly') }}
),
payroll_agg as (
    select
        CalendarYear,
        CalendarMonth,
        MonthName,
        SchoolYear,
        EtablissementID,
        Etablissement,
        NiveauID,
        Niveau,
        ClasseID,
        Classe,
        ClasseLibelle,
        sum(TotalPayrollAmount) as TotalPayrollAmount,
        sum(PersonnelCount) as PersonnelCount
    from payroll
    group by
        CalendarYear,
        CalendarMonth,
        MonthName,
        SchoolYear,
        EtablissementID,
        Etablissement,
        NiveauID,
        Niveau,
        ClasseID,
        Classe,
        ClasseLibelle
),
revenue_agg as (
    select
        CalendarYear,
        CalendarMonth,
        MonthName,
        SchoolYear,
        EtablissementID,
        Etablissement,
        NiveauID,
        Niveau,
        ClasseID,
        Classe,
        ClasseLibelle,
        sum(TotalBilledRevenue) as TotalBilledRevenue,
        sum(StudentCount) as StudentCount
    from revenue
    group by
        CalendarYear,
        CalendarMonth,
        MonthName,
        SchoolYear,
        EtablissementID,
        Etablissement,
        NiveauID,
        Niveau,
        ClasseID,
        Classe,
        ClasseLibelle
)

select
    coalesce(revenue_agg.CalendarYear, payroll_agg.CalendarYear) as CalendarYear,
    coalesce(revenue_agg.CalendarMonth, payroll_agg.CalendarMonth) as CalendarMonth,
    coalesce(revenue_agg.MonthName, payroll_agg.MonthName) as MonthName,
    coalesce(revenue_agg.SchoolYear, payroll_agg.SchoolYear) as SchoolYear,
    coalesce(revenue_agg.EtablissementID, payroll_agg.EtablissementID) as EtablissementID,
    coalesce(revenue_agg.Etablissement, payroll_agg.Etablissement) as Etablissement,
    coalesce(revenue_agg.NiveauID, payroll_agg.NiveauID) as NiveauID,
    coalesce(revenue_agg.Niveau, payroll_agg.Niveau) as Niveau,
    coalesce(revenue_agg.ClasseID, payroll_agg.ClasseID) as ClasseID,
    coalesce(revenue_agg.Classe, payroll_agg.Classe) as Classe,
    coalesce(revenue_agg.ClasseLibelle, payroll_agg.ClasseLibelle) as ClasseLibelle,
    coalesce(revenue_agg.TotalBilledRevenue, 0) as TotalBilledRevenue,
    coalesce(payroll_agg.TotalPayrollAmount, 0) as TotalPayrollAmount,
    coalesce(revenue_agg.StudentCount, 0) as StudentCount,
    coalesce(payroll_agg.PersonnelCount, 0) as PersonnelCount,
    coalesce(revenue_agg.TotalBilledRevenue, 0) - coalesce(payroll_agg.TotalPayrollAmount, 0) as GrossMargin,
    case
        when coalesce(revenue_agg.TotalBilledRevenue, 0) = 0 then null
        else (coalesce(revenue_agg.TotalBilledRevenue, 0) - coalesce(payroll_agg.TotalPayrollAmount, 0)) * 1.0
             / coalesce(revenue_agg.TotalBilledRevenue, 0)
    end as GrossMarginPct,
    case
        when coalesce(revenue_agg.TotalBilledRevenue, 0) = 0 then null
        else coalesce(payroll_agg.TotalPayrollAmount, 0) * 1.0
             / coalesce(revenue_agg.TotalBilledRevenue, 0)
    end as PayrollPctOfRevenue
from revenue_agg
full outer join payroll_agg
    on revenue_agg.CalendarYear = payroll_agg.CalendarYear
   and revenue_agg.CalendarMonth = payroll_agg.CalendarMonth
   and revenue_agg.SchoolYear = payroll_agg.SchoolYear
   and coalesce(revenue_agg.EtablissementID, -1) = coalesce(payroll_agg.EtablissementID, -1)
   and coalesce(revenue_agg.NiveauID, -1) = coalesce(payroll_agg.NiveauID, -1)
   and coalesce(revenue_agg.ClasseID, -1) = coalesce(payroll_agg.ClasseID, -1)