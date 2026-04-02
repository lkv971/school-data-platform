CREATE FUNCTION LISE.fn_RevenueParNiveau (@Year INT = NULL, @Month INT = NULL)
RETURNS TABLE 
AS
RETURN 
SELECT n.Niveau, SUM(fe.TotalEleve) AS TotalRevenue
FROM LISE.Niveaux AS n
JOIN LISE.Classes AS c
ON n.NiveauID = c.NiveauID
JOIN LISE.FacturesEleves AS fe
ON fe.ClasseID = c.ClasseID
WHERE (YEAR(fe.DateFacture) IS NULL OR YEAR(fe.DateFacture) = @Year)
AND (MONTH(fe.DateFacture) IS NULL OR MONTH(fe.DateFacture) = @Month)
GROUP BY n.Niveau