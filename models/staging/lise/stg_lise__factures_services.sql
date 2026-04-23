select
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
from {{ source('lise', 'FacturesServices') }}