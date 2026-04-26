select
    ClasseID,
    Classe,
    ClasseLibelle,
    NiveauID,
    EtablissementID
from {{ source('lise', 'Classes') }}