-- Auto Generated (Do not modify) 0BFE4846604B926FF1E5EBF8E6A42891B432C45280B27F81F8E7F15DB31D8E50
CREATE   VIEW LISE.vw_MartPayrollMonthly
AS
SELECT
    CalendarYear,
    CalendarMonth,
    MonthName,
    SchoolYear,
    EtablissementID,
    Etablissement,
    NiveauID,
    Niveau,
    ClasseID,
    Classe,
    ClasseLibelle,
    ProfessionTypeID,
    ProfessionType,
    COUNT(*) AS PayrollLineCount,
    COUNT(DISTINCT PersonnelID) AS PersonnelCount,
    SUM(Montant) AS TotalPayrollAmount,
    AVG(Montant) AS AvgPayrollAmount
FROM LISE.vw_PayrollDetailed
GROUP BY
    CalendarYear,
    CalendarMonth,
    MonthName,
    SchoolYear,
    EtablissementID,
    Etablissement,
    NiveauID,
    Niveau,
    ClasseID,
    Classe,
    ClasseLibelle,
    ProfessionTypeID,
    ProfessionType;