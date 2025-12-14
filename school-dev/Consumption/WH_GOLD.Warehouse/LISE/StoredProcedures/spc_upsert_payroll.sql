CREATE PROCEDURE LISE.spc_upsert_payroll
AS
BEGIN
    SET NOCOUNT ON;

    MERGE LISE.Payroll AS T
    USING (
        SELECT PersonnelID, PersonnelKey, Nom, Prenom, Periode, 
        Date, Montant, Devise, ProfessionTypeID,
        EtablissementID, ClasseID 
        FROM LISE.Payroll_Incoming) AS S 
    ON T.PersonnelID = S.PersonnelID
    AND T.PersonnelKey = S.PersonnelKey
    AND T.Nom = S.Nom
    AND T.Prenom = S.Prenom
    AND T.Periode = S.Periode
    AND T.Date = S.Date
    AND T.ProfessionTypeID = S.ProfessionTypeID
    AND T.EtablissementID = S.EtablissementID
    AND COALESCE(T.ClasseID, 0) = COALESCE(S.ClasseID, 0)
    WHEN MATCHED THEN
    UPDATE SET
        T.Montant = S.Montant,
        T.Devise = S.Devise
    WHEN NOT MATCHED THEN 
        INSERT (PaieID, PersonnelID, PersonnelKey, Nom, Prenom, Periode, Date, Montant, Devise, ProfessionTypeID,
                EtablissementID, ClasseID)
        VALUES (NEWID(), S.PersonnelID, S.PersonnelKey, S.Nom, S.Prenom, S.Periode, S.Date, S.Montant, S.Devise, S.ProfessionTypeID,
                S.EtablissementID, S.ClasseID);

    TRUNCATE TABLE LISE.Payroll_Incoming;
END;