select
    ProfessionTypeID,
    ProfessionType
from {{ source('lise', 'ProfessionTypes') }}