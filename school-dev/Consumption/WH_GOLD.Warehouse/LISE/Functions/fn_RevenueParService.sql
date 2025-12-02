CREATE FUNCTION LISE.fn_RevenueParService (@Year INT = NULL, @Month INT = NULL)
RETURNS TABLE
AS 
RETURN
SELECT ls.Service, SUM(lfs.TotalService) AS TotalRevenue
FROM LISE.FacturesServices AS lfs
JOIN LISE.Services AS ls
ON ls.ServiceID = lfs.ServiceID
WHERE (YEAR(lfs.DateFacture) IS NULL OR YEAR(lfs.DateFacture) = @Year)
AND (MONTH(lfs.DateFacture) IS NULL OR MONTH(lfs.DateFacture) = @Month)
GROUP BY ls.Service;