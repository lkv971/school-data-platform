-- Auto Generated (Do not modify) BCE882433F7BC72AEED88C3E36A51C25A8A3E5F99ADA860A5E4F0FAE219F18E2
CREATE   VIEW LISE.vw_MartPayrollPersonnel
AS
SELECT
    PersonnelID,
    FullName,
    ProfessionTypeID,
    ProfessionType,
    EtablissementID,
    Etablissement,
    NiveauID,
    Niveau,
    ClasseID,
    Classe,
    ClasseLibelle,
    CalendarYear,
    CalendarMonth,
    MonthName,
    SchoolYear,
    COUNT(*) AS PayrollLineCount,
    SUM(Montant) AS TotalPayrollAmount,
    AVG(Montant) AS AvgPayrollAmount,
    MIN(PayrollDate) AS FirstPayrollDate,
    MAX(PayrollDate) AS LastPayrollDate
FROM LISE.vw_PayrollDetailed
GROUP BY
    PersonnelID,
    FullName,
    ProfessionTypeID,
    ProfessionType,
    EtablissementID,
    Etablissement,
    NiveauID,
    Niveau,
    ClasseID,
    Classe,
    ClasseLibelle,
    CalendarYear,
    CalendarMonth,
    MonthName,
    SchoolYear;