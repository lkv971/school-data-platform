select
    NiveauID,
    Niveau,
    EtablissementID
from {{ source('lise', 'Niveaux') }}