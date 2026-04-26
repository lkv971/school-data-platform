select
    EleveKey,
    ResponsableKey,
    ValidationKey,
    ClasseKey,
    ClasseID,
    RegimeID,
    DateFacture,
    count(*) as row_count
from {{ ref('stg_lise__factures_eleves') }}
group by
    EleveKey,
    ResponsableKey,
    ValidationKey,
    ClasseKey,
    ClasseID,
    RegimeID,
    DateFacture
having count(*) > 1