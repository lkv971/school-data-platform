-- Auto Generated (Do not modify) 0E9D7765B9BD57187E6871E2274ED368866CD7031BE553826F6EB69B77305D8A
CREATE   VIEW LISE.vw_PayrollDetailed
AS
SELECT
    P.PaieID,
    P.PersonnelID,
    CONCAT(P.Nom, ' ', P.Prenom) AS FullName,
    P.Nom,
    P.Prenom,
    P.Periode,
    P.[Date] AS PayrollDate,
    D.CalendarYear,
    D.CalendarMonth,
    D.MonthName,
    D.SchoolYear,
    P.Montant,
    P.Devise,
    P.ProfessionTypeID,
    PT.ProfessionType,
    P.EtablissementID,
    E.Etablissement,
    P.ClasseID,
    C.Classe,
    C.ClasseLibelle,
    COALESCE(P.NiveauID, C.NiveauID) AS NiveauID,
    N.Niveau
FROM LISE.Payroll AS P
LEFT JOIN LISE.ProfessionTypes AS PT
    ON P.ProfessionTypeID = PT.ProfessionTypeID
LEFT JOIN LISE.Etablissements AS E
    ON P.EtablissementID = E.EtablissementID
LEFT JOIN LISE.Classes AS C
    ON P.ClasseID = C.ClasseID
LEFT JOIN LISE.Niveaux AS N
    ON COALESCE(P.NiveauID, C.NiveauID) = N.NiveauID
LEFT JOIN LISE.Dates AS D
    ON P.[Date] = D.[Date];