-- Auto Generated (Do not modify) A77EA4A67DF763C430EFC952367CCF06B8B8A0E9E94F8387C1417017B9CE463B
create view "dbt_school_dev"."stg_lise__factures_services" as select
    EleveKey,
    EleveID,
    ResponsableKey,
    ResponsableID,
    ValidationKey,
    ValidationID,
    ServiceID,
    Quantite,
    Prix,
    Remise,
    TotalService,
    DateFacture
from "WH_GOLD"."LISE"."FacturesServices";