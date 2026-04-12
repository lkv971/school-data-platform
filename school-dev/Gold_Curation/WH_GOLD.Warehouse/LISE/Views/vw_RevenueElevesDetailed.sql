-- Auto Generated (Do not modify) B610C067989F5BA4FD78F2659937312ECF7A24894CEEC1066DE507676F936B01
CREATE   VIEW LISE.vw_RevenueElevesDetailed
AS
SELECT
    FE.EleveKey,
    FE.EleveID,
    FE.ResponsableKey,
    FE.ResponsableID,
    FE.ValidationKey,
    FE.ValidationID,
    FE.ClasseKey,
    FE.ClasseID,
    FE.RegimeID,
    FE.TotalEleve,
    FE.DateFacture,
    D.CalendarYear,
    D.CalendarMonth,
    D.MonthName,
    D.SchoolYear,
    C.Classe,
    C.ClasseLibelle,
    C.NiveauID,
    N.Niveau,
    C.EtablissementID,
    E.Etablissement,
    R.Regime
FROM LISE.FacturesEleves AS FE
LEFT JOIN LISE.Dates AS D
    ON FE.DateFacture = D.[Date]
LEFT JOIN LISE.Classes AS C
    ON FE.ClasseID = C.ClasseID
LEFT JOIN LISE.Niveaux AS N
    ON C.NiveauID = N.NiveauID
LEFT JOIN LISE.Etablissements AS E
    ON C.EtablissementID = E.EtablissementID
LEFT JOIN LISE.Regimes AS R
    ON FE.RegimeID = R.RegimeID;