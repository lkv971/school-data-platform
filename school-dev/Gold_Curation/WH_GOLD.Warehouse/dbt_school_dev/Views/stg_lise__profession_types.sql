-- Auto Generated (Do not modify) E27F53173E64B8248ED5E8F2308D1189F08368B4ADF4FB0160B6173384E1BB4C
create view "dbt_school_dev"."stg_lise__profession_types" as select
    ProfessionTypeID,
    ProfessionType
from "WH_GOLD"."LISE"."ProfessionTypes";