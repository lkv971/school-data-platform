-- Auto Generated (Do not modify) 248FA56260B34B0330B233D60F1E9B76C31D72BFE104CA8D3BB01E35FB37BCDF
create view "dbt_school_dev"."stg_lise__factures_eleves" as select
    EleveKey,
    EleveID,
    ResponsableKey,
    ResponsableID,
    ValidationKey,
    ValidationID,
    ClasseKey,
    ClasseID,
    RegimeID,
    TotalEleve,
    DateFacture
from "WH_GOLD"."LISE"."FacturesEleves";