-- Auto Generated (Do not modify) 15473686C8A3A375C3415CDDBBD177C4894E11A4B4DEFAB2AF5D3C2D46DA887A
CREATE   VIEW LISE.vw_MartRevenueFamillesMonthly
AS
SELECT
    CalendarYear,
    CalendarMonth,
    MonthName,
    SchoolYear,
    Ville,
    Profession,
    Reglement,
    Banque,
    COUNT(*) AS FamilyInvoiceLineCount,
    COUNT(DISTINCT ResponsableID) AS FamilyCount,
    SUM(TotalFamille) AS TotalFamilyBilledRevenue,
    AVG(TotalFamille) AS AvgFamilyBilledRevenue
FROM LISE.vw_RevenueFamillesDetailed
GROUP BY
    CalendarYear,
    CalendarMonth,
    MonthName,
    SchoolYear,
    Ville,
    Profession,
    Reglement,
    Banque;