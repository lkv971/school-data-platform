select
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
from {{ source('lise', 'FacturesEleves') }}