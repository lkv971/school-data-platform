-- Auto Generated (Do not modify) 4FCA1E5FD3DEDD9E8ED8C165B2ED1213AEB6503D3C3AEC403E44686043A5F6AB
CREATE   VIEW LISE.Employees
AS
WITH StaffLatest AS (
    SELECT
        s.*,
        ROW_NUMBER() OVER (
            PARTITION BY s.PersonnelID
            ORDER BY s.DateEntree DESC, s.PersonnelKey DESC
        ) AS rn
    FROM LISE.Staff s
)
SELECT
    pe.PersonnelID,
    pe.Nom,
    pe.Prenom,
    pe.Nationalite,
    pe.Badge,

    sl.PersonnelKey,
    sl.Ville,
    sl.DateEntree,
    sl.DateSortie,
    sl.Telephone,
    sl.Email,
    sl.DateNaissance,
    sl.Age,

    CAST(CASE WHEN EXISTS (
        SELECT 1 FROM LISE.Professeurs pr WHERE pr.PersonnelID = pe.PersonnelID
    ) THEN 1 ELSE 0 END AS bit) AS IsTeacher
FROM LISE.Personnels pe
LEFT JOIN StaffLatest sl
    ON pe.PersonnelID = sl.PersonnelID
   AND sl.rn = 1;