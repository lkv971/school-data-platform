select
    EtablissementID,
    Etablissement
from {{ source('lise', 'Etablissements') }}