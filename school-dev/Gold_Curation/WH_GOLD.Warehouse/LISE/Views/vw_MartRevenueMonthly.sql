-- Auto Generated (Do not modify) 0D9A58725C5934C59E3ED774DAAD1ADCB15DFE53C04E989AFEA87F9E6E98E22F
CREATE   VIEW LISE.vw_MartRevenueMonthly
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
    RegimeID,
    Regime,
    COUNT(*) AS RevenueLineCount,
    COUNT(DISTINCT EleveID) AS StudentCount,
    SUM(TotalEleve) AS TotalBilledRevenue,
    AVG(TotalEleve) AS AvgBilledPerLine
FROM LISE.vw_RevenueElevesDetailed
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
    RegimeID,
    Regime;