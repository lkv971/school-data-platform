-- Auto Generated (Do not modify) CFA7AE5319B0D753CCBD4ECD8C5068696399D2A6FE025721C844186A42BDDB6D
CREATE   VIEW LISE.vw_MartRevenueValidations
AS
SELECT
    D.CalendarYear,
    D.CalendarMonth,
    D.MonthName,
    D.SchoolYear,
    FV.TypeFacture,
    COUNT(*) AS ValidationLineCount,
    SUM(FV.NombreFacture) AS TotalInvoicesValidated
FROM LISE.FacturesValidations AS FV
LEFT JOIN LISE.Dates AS D
    ON FV.DateValidation = D.[Date]
GROUP BY
    D.CalendarYear,
    D.CalendarMonth,
    D.MonthName,
    D.SchoolYear,
    FV.TypeFacture;