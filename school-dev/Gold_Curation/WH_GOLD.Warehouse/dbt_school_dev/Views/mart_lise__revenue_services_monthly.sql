-- Auto Generated (Do not modify) 85678805340C50DE24C42D2EFD4FF270BAFDC7BAE024C0B9544DBCD0818E9E7F
create view "dbt_school_dev"."mart_lise__revenue_services_monthly" as with factures_services as (
    select * from "WH_GOLD"."dbt_school_dev"."stg_lise__factures_services"
),
factures_eleves as (
    select * from "WH_GOLD"."dbt_school_dev"."stg_lise__factures_eleves"
),
dates as (
    select * from "WH_GOLD"."dbt_school_dev"."stg_lise__dates"
),
services as (
    select * from "WH_GOLD"."dbt_school_dev"."stg_lise__services"
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
fe_map as (
    select distinct
        EleveKey,
        EleveID,
        ResponsableKey,
        ResponsableID,
        ValidationKey,
        ValidationID,
        ClasseID
    from factures_eleves
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
    fe_map.ClasseID,
    classes.Classe,
    classes.ClasseLibelle,
    factures_services.ServiceID,
    services.Service,
    count(*) as ServiceLineCount,
    count(distinct factures_services.EleveKey) as StudentCount,
    sum(factures_services.Quantite) as TotalQuantity,
    sum(factures_services.TotalService) as TotalServiceRevenue,
    avg(factures_services.TotalService) as AvgServiceRevenuePerLine
from factures_services
left join dates
    on factures_services.DateFacture = dates.[Date]
left join services
    on factures_services.ServiceID = services.ServiceID
left join fe_map
    on factures_services.EleveKey = fe_map.EleveKey
   and factures_services.EleveID = fe_map.EleveID
   and factures_services.ResponsableKey = fe_map.ResponsableKey
   and factures_services.ResponsableID = fe_map.ResponsableID
   and factures_services.ValidationKey = fe_map.ValidationKey
   and factures_services.ValidationID = fe_map.ValidationID
left join classes
    on fe_map.ClasseID = classes.ClasseID
left join niveaux
    on classes.NiveauID = niveaux.NiveauID
left join etablissements
    on classes.EtablissementID = etablissements.EtablissementID
group by
    dates.CalendarYear,
    dates.CalendarMonth,
    dates.MonthName,
    dates.SchoolYear,
    classes.EtablissementID,
    etablissements.Etablissement,
    classes.NiveauID,
    niveaux.Niveau,
    fe_map.ClasseID,
    classes.Classe,
    classes.ClasseLibelle,
    factures_services.ServiceID,
    services.Service;