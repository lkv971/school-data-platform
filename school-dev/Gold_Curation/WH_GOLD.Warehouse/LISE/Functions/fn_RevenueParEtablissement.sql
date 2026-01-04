CREATE FUNCTION LISE.fn_RevenueParEtablissement (@Year INT = NULL, @Month INT = NULL)
RETURNS TABLE
AS 
RETURN
SELECT et.Etablissement, SUM(fe.TotalEleve) AS TotalRevenue
FROM LISE.Etablissements AS et 
JOIN LISE.Classes AS c
ON et.EtablissementID = c.EtablissementID
JOIN LISE.FacturesEleves AS fe
ON fe.ClasseID = c.ClasseID
WHERE (YEAR(fe.DateFacture) IS NULL OR YEAR(fe.DateFacture) = @Year)
AND (MONTH(fe.DateFacture) IS NULL OR MONTH(fe.DateFacture) = @Month)
GROUP BY et.Etablissement;