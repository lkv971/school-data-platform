-- Auto Generated (Do not modify) 0D9A58725C5934C59E3ED774DAAD1ADCB15DFE53C04E989AFEA87F9E6E98E22F
create view "dbt_school_dev"."mart_lise__revenue_monthly" as with factures_eleves as (
    select * from "WH_GOLD"."dbt_school_dev"."stg_lise__factures_eleves"
),
dates as (
    select * from "WH_GOLD"."dbt_school_dev"."stg_lise__dates"
),
classes as (
    select * from "WH_GOLD"."dbt_school_dev"."stg_lise__classes"
),
niveaux as (
    select * from "WH_GOLD"."dbt_school_dev"."stg_lise__niveaux"
),
etablissements as (
    select * from "WH_GOLD"."dbt_school_dev"."stg_lise__etablissements"
),
regimes as (
    select * from "WH_GOLD"."dbt_school_dev"."stg_lise__regimes"
)

select
    dates.CalendarYear,
    dates.CalendarMonth,
    dates.MonthName,
    dates.SchoolYear,
    classes.EtablissementID,
    etablissements.Etablissement,
    classes.NiveauID,
    niveaux.Niveau,
    factures_eleves.ClasseID,
    classes.Classe,
    classes.ClasseLibelle,
    factures_eleves.RegimeID,
    regimes.Regime,
    count(*) as RevenueLineCount,
    count(distinct factures_eleves.EleveKey) as StudentCount,
    sum(factures_eleves.TotalEleve) as TotalBilledRevenue,
    avg(factures_eleves.TotalEleve) as AvgBilledPerLine
from factures_eleves
left join dates
    on factures_eleves.DateFacture = dates.[Date]
left join classes
    on factures_eleves.ClasseID = classes.ClasseID
left join niveaux
    on classes.NiveauID = niveaux.NiveauID
left join etablissements
    on classes.EtablissementID = etablissements.EtablissementID
left join regimes
    on factures_eleves.RegimeID = regimes.RegimeID
group by
    dates.CalendarYear,
    dates.CalendarMonth,
    dates.MonthName,
    dates.SchoolYear,
    classes.EtablissementID,
    etablissements.Etablissement,
    classes.NiveauID,
    niveaux.Niveau,
    factures_eleves.ClasseID,
    classes.Classe,
    classes.ClasseLibelle,
    factures_eleves.RegimeID,
    regimes.Regime;