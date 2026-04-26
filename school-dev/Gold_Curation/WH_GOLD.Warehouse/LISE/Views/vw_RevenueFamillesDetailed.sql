-- Auto Generated (Do not modify) 48782CADF751193FA4D0F82C695E6E0C64EEA083F0A9D86B6ECF4CB0EB9579EF
CREATE   VIEW LISE.vw_RevenueFamillesDetailed
AS
SELECT
    FF.ResponsableKey,
    FF.ResponsableID,
    FF.ValidationKey,
    FF.ValidationID,
    FF.FoyerID,
    FF.ProfessionID,
    FF.TotalFamille,
    FF.DateFacture,
    D.CalendarYear,
    D.CalendarMonth,
    D.MonthName,
    D.SchoolYear,
    F.Ville,
    P.Profession,
    PA.Nom,
    PA.Prenom,
    R.Reglement,
    R.Banque
FROM LISE.FacturesFamilles AS FF
LEFT JOIN LISE.Dates AS D
    ON FF.DateFacture = D.[Date]
LEFT JOIN LISE.Foyers AS F
    ON FF.FoyerID = F.FoyerID
LEFT JOIN LISE.Professions AS P
    ON FF.ProfessionID = P.ProfessionID
LEFT JOIN LISE.Parents AS PA
    ON FF.ResponsableID = PA.ResponsableID
LEFT JOIN LISE.Responsables AS R
    ON FF.ResponsableKey = R.ResponsableKey;