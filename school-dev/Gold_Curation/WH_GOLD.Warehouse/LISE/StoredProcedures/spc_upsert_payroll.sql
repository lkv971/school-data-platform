CREATE   PROCEDURE LISE.spc_upsert_payroll
AS
BEGIN
    SET NOCOUNT ON;

    MERGE LISE.Payroll AS T
    USING (
        SELECT
            PersonnelID,
            Nom,
            Prenom,
            Periode,
            [Date],
            Montant,
            Devise,
            ProfessionTypeID,
            EtablissementID,
            ClasseID,
            NiveauID
        FROM LISE.Payroll_Incoming
    ) AS S
    ON  T.PersonnelID = S.PersonnelID
    AND T.Periode = S.Periode
    AND T.[Date] = S.[Date]
    AND T.ProfessionTypeID = S.ProfessionTypeID
    AND T.EtablissementID = S.EtablissementID
    AND COALESCE(T.ClasseID, -1) = COALESCE(S.ClasseID, -1)
    AND COALESCE(T.NiveauID, -1) = COALESCE(S.NiveauID, -1)

    WHEN MATCHED THEN
        UPDATE SET
            T.Nom = S.Nom,
            T.Prenom = S.Prenom,
            T.Montant = S.Montant,
            T.Devise = S.Devise,
            T.ClasseID = S.ClasseID,
            T.NiveauID = S.NiveauID

    WHEN NOT MATCHED THEN
        INSERT (
            PaieID,
            PersonnelID,
            Nom,
            Prenom,
            Periode,
            [Date],
            Montant,
            Devise,
            ProfessionTypeID,
            EtablissementID,
            ClasseID,
            NiveauID
        )
        VALUES (
            NEWID(),
            S.PersonnelID,
            S.Nom,
            S.Prenom,
            S.Periode,
            S.[Date],
            S.Montant,
            S.Devise,
            S.ProfessionTypeID,
            S.EtablissementID,
            S.ClasseID,
            S.NiveauID
        );

    TRUNCATE TABLE LISE.Payroll_Incoming;
END;