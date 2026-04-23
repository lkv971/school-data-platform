select
    PaieID,
    PersonnelID,
    Nom,
    Prenom,
    Periode,
    Date,
    Montant,
    Devise,
    ProfessionTypeID,
    EtablissementID,
    ClasseID,
    NiveauID
from {{ source('lise', 'Payroll') }}