-- Auto Generated (Do not modify) 49274B153EC38018B05512E9533DF8DB7D7AD69F91EDE24CEE39FCE3A2E17C07
CREATE   VIEW LISE.vw_MartMarginByProfessionType
AS
WITH PayrollTypeBase AS (
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
        ClasseLibelle,
        ProfessionTypeID,
        ProfessionType
),
PayrollTypeShare AS (
    SELECT
        PTB.*,
        SUM(PTB.TotalPayrollAmount) OVER (
            PARTITION BY
                PTB.CalendarYear,
                PTB.CalendarMonth,
                PTB.SchoolYear,
                PTB.EtablissementID,
                PTB.NiveauID,
                PTB.ClasseID
        ) AS TotalPayrollAllTypes
    FROM PayrollTypeBase AS PTB
)
SELECT
    PTS.CalendarYear,
    PTS.CalendarMonth,
    PTS.MonthName,
    PTS.SchoolYear,
    PTS.EtablissementID,
    PTS.Etablissement,
    PTS.NiveauID,
    PTS.Niveau,
    PTS.ClasseID,
    PTS.Classe,
    PTS.ClasseLibelle,
    PTS.ProfessionTypeID,
    PTS.ProfessionType,
    PTS.PersonnelCount,
    PTS.TotalPayrollAmount,
    COALESCE(MM.TotalBilledRevenue, 0) AS TotalBilledRevenueAtClassMonth,
    CASE
        WHEN COALESCE(PTS.TotalPayrollAllTypes, 0) = 0 THEN NULL
        ELSE PTS.TotalPayrollAmount / PTS.TotalPayrollAllTypes
    END AS PayrollShareWithinClassMonth,
    CASE
        WHEN COALESCE(PTS.TotalPayrollAllTypes, 0) = 0 THEN NULL
        ELSE COALESCE(MM.TotalBilledRevenue, 0) * (PTS.TotalPayrollAmount / PTS.TotalPayrollAllTypes)
    END AS AllocatedRevenue,
    CASE
        WHEN COALESCE(PTS.TotalPayrollAllTypes, 0) = 0 THEN NULL
        ELSE (COALESCE(MM.TotalBilledRevenue, 0) * (PTS.TotalPayrollAmount / PTS.TotalPayrollAllTypes))
             - PTS.TotalPayrollAmount
    END AS AllocatedGrossMargin,
    CASE
        WHEN COALESCE(PTS.TotalPayrollAllTypes, 0) = 0 THEN NULL
        WHEN COALESCE(MM.TotalBilledRevenue, 0) = 0 THEN NULL
        ELSE (
            (COALESCE(MM.TotalBilledRevenue, 0) * (PTS.TotalPayrollAmount / PTS.TotalPayrollAllTypes))
            - PTS.TotalPayrollAmount
        ) / NULLIF(COALESCE(MM.TotalBilledRevenue, 0) * (PTS.TotalPayrollAmount / PTS.TotalPayrollAllTypes), 0)
    END AS AllocatedGrossMarginPct
FROM PayrollTypeShare AS PTS
LEFT JOIN LISE.vw_MartMarginMonthly AS MM
    ON PTS.CalendarYear = MM.CalendarYear
   AND PTS.CalendarMonth = MM.CalendarMonth
   AND PTS.SchoolYear = MM.SchoolYear
   AND COALESCE(PTS.EtablissementID, -1) = COALESCE(MM.EtablissementID, -1)
   AND COALESCE(PTS.NiveauID, -1) = COALESCE(MM.NiveauID, -1)
   AND COALESCE(PTS.ClasseID, -1) = COALESCE(MM.ClasseID, -1);