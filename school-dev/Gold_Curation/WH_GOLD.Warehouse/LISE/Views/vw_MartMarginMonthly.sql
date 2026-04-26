-- Auto Generated (Do not modify) B5C6CC5343FA959530EEBE17C6540E7D47672ABFC621E1FCB576C73F3838A2E1
CREATE   VIEW LISE.vw_MartMarginMonthly
AS
WITH PayrollAgg AS (
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
        SUM(TotalPayrollAmount) AS TotalPayrollAmount,
        SUM(PersonnelCount) AS PersonnelCount
    FROM LISE.vw_MartPayrollMonthly
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
        ClasseLibelle
),
RevenueAgg AS (
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
        SUM(TotalBilledRevenue) AS TotalBilledRevenue,
        SUM(StudentCount) AS StudentCount
    FROM LISE.vw_MartRevenueMonthly
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
        ClasseLibelle
)
SELECT
    COALESCE(R.CalendarYear, P.CalendarYear) AS CalendarYear,
    COALESCE(R.CalendarMonth, P.CalendarMonth) AS CalendarMonth,
    COALESCE(R.MonthName, P.MonthName) AS MonthName,
    COALESCE(R.SchoolYear, P.SchoolYear) AS SchoolYear,
    COALESCE(R.EtablissementID, P.EtablissementID) AS EtablissementID,
    COALESCE(R.Etablissement, P.Etablissement) AS Etablissement,
    COALESCE(R.NiveauID, P.NiveauID) AS NiveauID,
    COALESCE(R.Niveau, P.Niveau) AS Niveau,
    COALESCE(R.ClasseID, P.ClasseID) AS ClasseID,
    COALESCE(R.Classe, P.Classe) AS Classe,
    COALESCE(R.ClasseLibelle, P.ClasseLibelle) AS ClasseLibelle,
    COALESCE(R.TotalBilledRevenue, 0) AS TotalBilledRevenue,
    COALESCE(P.TotalPayrollAmount, 0) AS TotalPayrollAmount,
    COALESCE(R.StudentCount, 0) AS StudentCount,
    COALESCE(P.PersonnelCount, 0) AS PersonnelCount,
    COALESCE(R.TotalBilledRevenue, 0) - COALESCE(P.TotalPayrollAmount, 0) AS GrossMargin,
    CASE
        WHEN COALESCE(R.TotalBilledRevenue, 0) = 0 THEN NULL
        ELSE
            (COALESCE(R.TotalBilledRevenue, 0) - COALESCE(P.TotalPayrollAmount, 0))
            / COALESCE(R.TotalBilledRevenue, 0)
    END AS GrossMarginPct,
    CASE
        WHEN COALESCE(R.TotalBilledRevenue, 0) = 0 THEN NULL
        ELSE COALESCE(P.TotalPayrollAmount, 0) / COALESCE(R.TotalBilledRevenue, 0)
    END AS PayrollPctOfRevenue
FROM RevenueAgg AS R
FULL OUTER JOIN PayrollAgg AS P
    ON R.CalendarYear = P.CalendarYear
   AND R.CalendarMonth = P.CalendarMonth
   AND R.SchoolYear = P.SchoolYear
   AND COALESCE(R.EtablissementID, -1) = COALESCE(P.EtablissementID, -1)
   AND COALESCE(R.NiveauID, -1) = COALESCE(P.NiveauID, -1)
   AND COALESCE(R.ClasseID, -1) = COALESCE(P.ClasseID, -1);