select
    RegimeID,
    Regime
from {{ source('lise', 'Regimes') }}