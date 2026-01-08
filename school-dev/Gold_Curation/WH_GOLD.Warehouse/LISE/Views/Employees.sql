-- Auto Generated (Do not modify) E19BB02ACDB7BA437ADEADEAA3BE60EC3501720875F6704B342940147A202042
CREATE   VIEW LISE.Employees
AS SELECT pe.PersonnelID, s.PersonnelKey, pr.ProfesseurID, pe.Nom, pe.Prenom, pr.ClasseID, pe.Nationalite, s.Ville, s.DateEntree, s.DateSortie, s.Telephone,
s.Email, s.DateNaissance, s.Age, pe.Badge
FROM LISE.Personnels AS pe
LEFT JOIN LISE.Staff AS s
ON pe.PersonnelID = s.PersonnelID
LEFT JOIN LISE.Professeurs AS pr
ON pe.PersonnelID = pr.PersonnelID;