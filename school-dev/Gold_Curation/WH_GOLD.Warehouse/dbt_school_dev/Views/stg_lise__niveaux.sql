-- Auto Generated (Do not modify) DD2E787352585A8460DB3ED5C44F427107DDAD879C2F5A7BF92292B20796D8E6
create view "dbt_school_dev"."stg_lise__niveaux" as select
    NiveauID,
    Niveau,
    EtablissementID
from "WH_GOLD"."LISE"."Niveaux";