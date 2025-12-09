-- Auto Generated (Do not modify) 1A1566E24B4A455872A31DCAC8720BEF56BAE4D9DE28B1B0977C4AFB10443136
CREATE VIEW LISE.vDimClasses AS
SELECT
    c.ClasseID,
    c.Classe,
    c.ClasseLibelle,
    c.NiveauID,
    n.Niveau,
    c.EtablissementID,
    e.Etablissement
FROM LISE.Classes c
JOIN LISE.Niveaux n
    ON c.NiveauID = n.NiveauID
JOIN LISE.Etablissements e
    ON c.EtablissementID = e.EtablissementID;