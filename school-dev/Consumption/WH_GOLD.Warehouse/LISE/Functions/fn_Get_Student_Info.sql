CREATE FUNCTION LISE.fn_Get_Student_Info (@studentid INT)
RETURNS TABLE
AS RETURN
SELECT en.EleveID, en.Prenom, en.Nom, en.Sexe, el.DateEntree, el.DateSortie, el.ResponsableID, c.Classe, c.ClasseLibelle
FROM LISE.Enfants AS en
INNER JOIN LISE.Eleves AS el
ON en.EleveID = el.EleveID
INNER JOIN LISE.FacturesEleves AS fel
ON el.EleveKey = fel.EleveKey
INNER JOIN LISE.Classes AS c
ON c.ClasseID = fel.ClasseID
WHERE en.EleveID = @studentid;