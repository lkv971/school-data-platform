-- Auto Generated (Do not modify) 6D9A818D05CB07662884D477E204D560FC2F64F84531ADA3963110D7FFEB1A5E
create view "dbt_school_dev"."stg_lise__classes" as select
    ClasseID,
    Classe,
    ClasseLibelle,
    NiveauID,
    EtablissementID
from "WH_GOLD"."LISE"."Classes";