-- Auto Generated (Do not modify) 080B44F06A8C5ECA41EDFCFD6825D0FB331CEAFEF35777DD64950B79BB855691
 CREATE   VIEW LISE.vw_MartPayrollMix
AS
WITH Base AS (
    SELECT
        CalendarYear,
        CalendarMonth,
        MonthName,
        SchoolYear,
        ProfessionTypeID,
        ProfessionType,
        COUNT(DISTINCT PersonnelID) AS PersonnelCount,
        SUM(Montant) AS TotalPayrollAmount
    FROM LISE.vw_PayrollDetailed
    GROUP BY
        CalendarYear,
        CalendarMonth,
        MonthName,
        SchoolYear,
        ProfessionTypeID,
        ProfessionType
),
Totals AS (
    SELECT
        CalendarYear,
        CalendarMonth,
        SchoolYear,
        SUM(TotalPayrollAmount) AS TotalMonthPayroll
    FROM Base
    GROUP BY
        CalendarYear,
        CalendarMonth,
        SchoolYear
)
SELECT
    B.CalendarYear,
    B.CalendarMonth,
    B.MonthName,
    B.SchoolYear,
    B.ProfessionTypeID,
    B.ProfessionType,
    B.PersonnelCount,
    B.TotalPayrollAmount,
    T.TotalMonthPayroll,
    CASE
        WHEN T.TotalMonthPayroll = 0 THEN 0
        ELSE B.TotalPayrollAmount / T.TotalMonthPayroll
    END AS PayrollShareOfMonth
FROM Base AS B
INNER JOIN Totals AS T
    ON B.CalendarYear = T.CalendarYear
   AND B.CalendarMonth = T.CalendarMonth
   AND B.SchoolYear = T.SchoolYear;