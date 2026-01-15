CREATE   PROCEDURE LISE.spc_upsert_payroll
    @pPayPeriod varchar(7)  -- 'YYYY-MM' e.g. '2025-09'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @PayDate date =
        DATEFROMPARTS(CONVERT(int, LEFT(@pPayPeriod, 4)),
                      CONVERT(int, RIGHT(@pPayPeriod, 2)),
                      1);

    BEGIN TRY
        BEGIN TRAN;

        /* 1) DQ PRE-CHECK: Incoming duplicates for this month */
        IF EXISTS (
            SELECT 1
            FROM LISE.Payroll_Incoming
            WHERE [Date] = @PayDate
            GROUP BY PersonnelID, [Date]
            HAVING COUNT(*) > 1
        )
        BEGIN
            THROW 51010,
                  'DQ FAIL: Duplicate rows found in LISE.Payroll_Incoming for this month (PersonnelID + Date).',
                  1;
        END

        /* 2) Replace month in final table (idempotent reruns) */
        DELETE FROM LISE.Payroll
        WHERE [Date] = @PayDate;

        /* 3) Insert month */
        INSERT INTO LISE.Payroll
        (
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
        SELECT
            NEWID(),
            pi.PersonnelID,
            pi.Nom,
            pi.Prenom,
            pi.Periode,
            pi.[Date],
            pi.Montant,
            pi.Devise,
            pi.ProfessionTypeID,
            pi.EtablissementID,
            pi.ClasseID,
            pi.NiveauID
        FROM LISE.Payroll_Incoming pi
        WHERE pi.[Date] = @PayDate;

        /* 4) DQ POST-CHECK: Final duplicates for this month */
        IF EXISTS (
            SELECT 1
            FROM LISE.Payroll
            WHERE [Date] = @PayDate
            GROUP BY PersonnelID, [Date]
            HAVING COUNT(*) > 1
        )
        BEGIN
            THROW 51011,
                  'DQ FAIL: Duplicate rows detected in LISE.Payroll after load for this month (PersonnelID + Date).',
                  1;
        END

        /* 5) Cleanup staging (recommended) */
        DELETE FROM LISE.Payroll_Incoming
        WHERE [Date] = @PayDate;

        COMMIT TRAN;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRAN;
        THROW;  -- makes UpsertPayroll activity fail -> pipeline fails
    END CATCH
END;