select
    EleveKey,
    ResponsableKey,
    ValidationKey,
    ServiceID,
    DateFacture,
    Quantite,
    Prix,
    Remise,
    TotalService,
    count(*) as row_count
from {{ ref('stg_lise__factures_services') }}
group by
    EleveKey,
    ResponsableKey,
    ValidationKey,
    ServiceID,
    DateFacture,
    Quantite,
    Prix,
    Remise,
    TotalService
having count(*) > 1