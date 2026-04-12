-- Auto Generated (Do not modify) 85678805340C50DE24C42D2EFD4FF270BAFDC7BAE024C0B9544DBCD0818E9E7F
CREATE   VIEW LISE.vw_MartRevenueServicesMonthly
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
    ServiceID,
    Service,
    COUNT(*) AS ServiceLineCount,
    COUNT(DISTINCT EleveID) AS StudentCount,
    SUM(Quantite) AS TotalQuantity,
    SUM(TotalService) AS TotalServiceRevenue,
    AVG(TotalService) AS AvgServiceRevenuePerLine
FROM LISE.vw_RevenueServicesDetailed
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
    ServiceID,
    Service;