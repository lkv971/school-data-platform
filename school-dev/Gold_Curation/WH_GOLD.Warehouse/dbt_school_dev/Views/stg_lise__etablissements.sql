-- Auto Generated (Do not modify) 7C22E2870B413CF5B7E8B3084A438B7CA1468588BAEE4043867EB9946B60CA3A
create view "dbt_school_dev"."stg_lise__etablissements" as select
    EtablissementID,
    Etablissement
from "WH_GOLD"."LISE"."Etablissements";