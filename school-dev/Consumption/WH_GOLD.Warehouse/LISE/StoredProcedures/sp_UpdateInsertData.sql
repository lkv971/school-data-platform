CREATE   PROCEDURE LISE.sp_UpdateInsertData
AS
BEGIN
  SET NOCOUNT ON;

    DECLARE
    @uclasses INT=0, @iclasses INT=0,
    @utargets INT=0, @itargets INT=0,
    @ueleves INT=0, @ieleves INT=0,
    @uprofessions INT=0, @iprofessions INT=0,
    @upays INT=0, @ipays INT=0,
    @ustaff INT=0, @istaff INT=0,
    @upersonnels INT=0, @ipersonnels INT=0,
    @uprofesseurs INT=0, @iprofesseurs INT=0,
    @uresponsables INT=0, @iresponsables INT=0,
    @univeaux INT=0, @iniveaux INT=0,
    @uetablissements INT=0, @ietablissements INT=0,
    @uregimes INT=0, @iregimes INT=0,
    @uparents INT=0, @iparents INT=0,
    @uenfants INT=0, @ienfants INT=0,
    @ufoyers INT=0, @ifoyers INT=0,
    @uservices INT=0, @iservices INT=0,
    @uvilles INT=0, @ivilles INT=0,
    @uschoolyear INT=0, @ischoolyear INT=0,
    @idates INT =0,
    @ifac_services INT=0,
    @ifac_niveaux INT=0,
    @ifac_eleves INT =0,
    @ifac_familles INT =0,
    @ifac_validations INT=0,
    @TotalRowsWritten INT=0; 

    UPDATE LC
    SET LC.Classe = LSC.CLASSE,
        LC.ClasseLibelle = LSC.CLASSELIBELLE
    FROM LISE.Classes AS LC
    INNER JOIN LISE.Staging_Classes AS LSC
      ON LC.ClasseID = LSC.IDCLASSE
    WHERE ISNULL(LC.Classe, '') <> ISNULL(LSC.CLASSE, '')
      OR ISNULL(LC.ClasseLibelle, '') <> ISNULL(LSC.CLASSELIBELLE, '')
    ;
    SET @uclasses = @@ROWCOUNT;

    INSERT INTO LISE.Classes (ClasseID, Classe, ClasseLibelle, NiveauID, EtablissementID)
    SELECT LSC.IDCLASSE, LSC.CLASSE, LSC.CLASSELIBELLE, LSC.IDNIVEAU, LSC.IDETABLISSEMENT
    FROM LISE.Staging_Classes AS LSC 
    WHERE NOT EXISTS (
      SELECT 1
      FROM LISE.Classes AS LC 
      WHERE LC.ClasseID = LSC.IDCLASSE
      )
    ;
    SET @iclasses = @@ROWCOUNT;
    
    UPDATE LCT 
    SET LCT.ClasseID = LSCT.IDCLASSE,
        LCT.TargetCount = LSCT.TARGETCOUNT,
        LCT.MaximumCount = LSCT.MAXIMUMCOUNT,
        LCT.SchoolYear = LSCT.SCHOOLYEAR
    FROM LISE.ClassesTargets AS LCT 
    INNER JOIN LISE.Staging_ClassesTargets AS LSCT
      ON LCT.ClasseKey = LSCT.KEYCLASSE
    WHERE ISNULL(LCT.ClasseID, '') <> ISNULL(LSCT.IDCLASSE, '')
      OR ISNULL(LCT.TargetCount, 0) <> ISNULL(LSCT.TARGETCOUNT, 0)
      OR ISNULL(LCT.MaximumCount, 0) <> ISNULL(LSCT.MAXIMUMCOUNT, 0)
      OR ISNULL(LCT.SchoolYear, '') <> ISNULL(LSCT.SCHOOLYEAR, '')
    ;
    SET @utargets = @@ROWCOUNT;
    
    INSERT INTO LISE.ClassesTargets (ClasseKey, ClasseID, TargetCount, MaximumCount, SchoolYear)
    SELECT LSCT.KEYCLASSE, LSCT.IDCLASSE, LSCT.TARGETCOUNT, LSCT.MAXIMUMCOUNT, LSCT.SCHOOLYEAR
    FROM LISE.Staging_ClassesTargets AS LSCT
    WHERE NOT EXISTS (
    SELECT 1 
    FROM LISE.ClassesTargets AS LCT
    WHERE LCT.ClasseKey = LSCT.KEYCLASSE
    )
    ;
    SET @itargets = @@ROWCOUNT;

    UPDATE LE
    SET 
        LE.EleveID = LSE.IDELEVE,
        LE.ResponsableID = LSE.IDRESPONSABLE,
        LE.DateEntree = LSE.DATEENTREE,
        LE.DateSortie = LSE.DATESORTIE
    FROM LISE.Eleves AS LE
    INNER JOIN LISE.Staging_Eleves AS LSE
      ON LE.EleveKey = LSE.KEYELEVE
    WHERE ISNULL(LE.EleveID, '') <> ISNULL(LSE.IDELEVE, '')
       OR ISNULL(LE.ResponsableID, '') <> ISNULL(LSE.IDRESPONSABLE, '')
       OR ISNULL(LE.DateEntree, '') <> ISNULL(LSE.DATEENTREE, '')
       OR ISNULL(LE.DateSortie, '') <> ISNULL(LSE.DATESORTIE, '')
    ;
    SET @ueleves = @@ROWCOUNT;

    INSERT INTO LISE.Eleves (EleveKey, EleveID, ResponsableID, DateEntree, DateSortie)
    SELECT LSE.KEYELEVE, LSE.IDELEVE, LSE.IDRESPONSABLE, LSE.DATEENTREE, LSE.DATESORTIE
    FROM LISE.Staging_Eleves AS LSE
    WHERE NOT EXISTS (
      SELECT 1
      FROM LISE.Eleves AS LE
      WHERE LE.EleveKey = LSE.KEYELEVE
      )
    ;
    SET @ieleves = @@ROWCOUNT;


    UPDATE LP
    SET LP.Profession = LSP.PROFESSION
    FROM LISE.Professions AS LP
    INNER JOIN LISE.Staging_Professions AS LSP
      ON LP.ProfessionID = LSP.IDPROFESSION
    WHERE ISNULL(LP.Profession, '') <> ISNULL(LSP.PROFESSION, '')
    ;
    SET @uprofessions = @@ROWCOUNT;

    INSERT INTO LISE.Professions (ProfessionID, Profession)
    SELECT LSP.IDPROFESSION, LSP.PROFESSION
    FROM LISE.Staging_Professions AS LSP
    WHERE NOT EXISTS ( 
      SELECT 1 FROM LISE.Professions AS LP
      WHERE LP.ProfessionID = LSP.IDPROFESSION
      )
    ;
    SET @iprofessions = @@ROWCOUNT;

    UPDATE LPAY
    SET LPAY.Pays = LSPAY.PAYS,
        LPAY.Nationalite = LSPAY.NATIONALITE
    FROM LISE.Pays AS LPAY
    INNER JOIN LISE.Staging_Pays AS LSPAY
    ON LPAY.PaysID = LSPAY.IDPAYS
    WHERE ISNULL(LPAY.Pays, '') <> ISNULL(LSPAY.PAYS, '')
      OR ISNULL(LPAY.Nationalite, '') <> ISNULL(LSPAY.NATIONALITE, '')
    ;
    SET @upays = @@ROWCOUNT;

    INSERT INTO LISE.Pays (PaysID, Pays, Nationalite)
    SELECT LSPAY.IDPAYS, LSPAY.PAYS, LSPAY.NATIONALITE
    FROM LISE.Staging_Pays AS LSPAY
    WHERE NOT EXISTS (
      SELECT 1 FROM LISE.Pays AS LPAY
      WHERE LPAY.PaysID = LSPAY.IDPAYS
      )
    ;
    SET @ipays = @@ROWCOUNT;

    UPDATE LST 
    SET LST.PersonnelID = LSST.IDPERSONNEL,
        LST.Ville = LSST.VILLE,
        LST.DateEntree = LSST.DATEENTREE,
        LST.DateSortie = LSST.DATESORTIE,
        LST.Telephone = LSST.TELEPHONE,
        LST.Email = LSST.EMAIL,
        LST.DateNaissance = LSST.DATENAISSANCE,
        LST.Age = LSST.AGE
    FROM LISE.Staff AS LST 
    INNER JOIN LISE.Staging_Staff AS LSST
    ON LST.PersonnelKey = LSST.KEYPERSONNEL
    WHERE ISNULL(LST.PersonnelID, '') <> ISNULL(LSST.IDPERSONNEL, '')
       OR ISNULL(LST.Ville, '') <> ISNULL(LSST.VILLE, '')
       OR ISNULL(LST.DateEntree, '') <> ISNULL(LSST.DATEENTREE, '')
       OR ISNULL(LST.DateSortie, '') <> ISNULL(LSST.DATESORTIE, '')
       OR ISNULL(LST.Telephone, '') <> ISNULL(LSST.TELEPHONE, '')
       OR ISNULL(LST.Email, '') <> ISNULL(LSST.EMAIL, '')
       OR ISNULL(LST.DateNaissance, '') <> ISNULL(LSST.DATENAISSANCE, '')
       OR ISNULL(LST.Age, -1) <> ISNULL(LSST.AGE, -1)
    ;
    SET @ustaff = @@ROWCOUNT;

    INSERT INTO LISE.Staff (PersonnelKey, PersonnelID, Ville, DateEntree, DateSortie, Telephone, Email, DateNaissance, Age)
    SELECT LSST.KEYPERSONNEL, LSST.IDPERSONNEL, LSST.VILLE, LSST.DATEENTREE, LSST.DATESORTIE, LSST.TELEPHONE, LSST.EMAIL, LSST.DATENAISSANCE, LSST.AGE
    FROM LISE.Staging_Staff AS LSST
    WHERE NOT EXISTS (
      SELECT 1 FROM LISE.Staff AS LST
      WHERE LST.PersonnelKey = LSST.KEYPERSONNEL
      )
    ;
    SET @istaff = @@ROWCOUNT;

    UPDATE LPS
    SET LPS.Nom = LSPS.NOM,
        LPS.Prenom = LSPS.PRENOM,
        LPS.Nationalite = LSPS.NATIONALITE,
        LPS.SecuriteSociale = LSPS.SECURITESOCIALE,
        LPS.Badge = LSPS.BADGE
    FROM LISE.Personnels AS LPS
    INNER JOIN LISE.Staging_Personnels AS LSPS
    ON LPS.PersonnelID = LSPS.IDPERSONNEL
    WHERE ISNULL(LPS.Nom, '') <> ISNULL(LSPS.NOM, '')
          OR ISNULL(LPS.Prenom, '') <> ISNULL(LSPS.PRENOM, '')
          OR ISNULL(LPS.Nationalite, '') <> ISNULL(LSPS.NATIONALITE, '')
          OR ISNULL(LPS.SecuriteSociale, '') <> ISNULL(LSPS.SECURITESOCIALE, '')
          OR ISNULL(LPS.Badge, '') <> ISNULL(LSPS.BADGE, '')
    ;
    SET @upersonnels = @@ROWCOUNT;

    INSERT INTO LISE.Personnels (PersonnelID, Nom, Prenom, Nationalite, SecuriteSociale, Badge)
    SELECT LSPS.IDPERSONNEL, LSPS.NOM, LSPS.PRENOM, LSPS.NATIONALITE, LSPS.SECURITESOCIALE, LSPS.BADGE
    FROM LISE.Staging_Personnels AS LSPS
    WHERE NOT EXISTS (
      SELECT 1 FROM LISE.Personnels AS LPS
      WHERE LPS.PersonnelID = LSPS.IDPERSONNEL
      )
    ;
    SET @ipersonnels = @@ROWCOUNT;

    UPDATE LPR
    SET LPR.PersonnelID = LSPR.IDPERSONNEL,
        LPR.ClasseID = LSPR.IDCLASSE
    FROM LISE.Professeurs AS LPR 
    INNER JOIN LISE.Staging_Professeurs AS LSPR
      ON LPR.ProfesseurID = LSPR.IDPROFESSEUR
    WHERE ISNULL(LPR.PersonnelID, '') <> ISNULL(LSPR.IDPERSONNEL, '')
          OR ISNULL(LPR.ClasseID, '') <> ISNULL(LSPR.IDCLASSE, '')
    ;
    SET @uprofesseurs = @@ROWCOUNT;

    INSERT INTO LISE.Professeurs (ProfesseurID, PersonnelID, ClasseID)
    SELECT LSPR.IDPROFESSEUR, LSPR.IDPERSONNEL, LSPR.IDCLASSE
    FROM LISE.Staging_Professeurs AS LSPR
    WHERE NOT EXISTS (
      SELECT 1 FROM LISE.Professeurs AS LPR
      WHERE LPR.ProfesseurID = LSPR.IDPROFESSEUR
      )
    ;
    SET @iprofesseurs = @@ROWCOUNT;

    UPDATE LR
    SET 
        LR.ResponsableID = LSR.IDRESPONSABLE,
        LR.EnfantsCharge = LSR.ENFANTSACHARGE,
        LR.Reglement = LSR.REGLEMENT,
        LR.Telephone = LSR.TELEPHONE,
        LR.Email = LSR.EMAIL,
        LR.NumeroCompte = LSR.NUMEROCOMPTE,
        LR.Banque = LSR.BANQUE
    FROM LISE.Responsables AS LR
    INNER JOIN LISE.Staging_Responsables AS LSR
      ON LR.ResponsableKey = LSR.KEYRESPONSABLE
    WHERE ISNULL(LR.ResponsableID, '') <> ISNULL(LSR.IDRESPONSABLE, '')
    ;
    SET @uresponsables = @@ROWCOUNT;

    INSERT INTO LISE.Responsables (ResponsableKey, ResponsableID, EnfantsCharge, Reglement, Telephone, Email, NumeroCompte, Banque)
    SELECT LSR.KEYRESPONSABLE, LSR.IDRESPONSABLE, LSR.ENFANTSACHARGE, LSR.REGLEMENT, LSR.TELEPHONE, LSR.EMAIL, LSR.NUMEROCOMPTE, LSR.BANQUE
    FROM LISE.Staging_Responsables AS LSR
    WHERE NOT EXISTS (
      SELECT 1
      FROM LISE.Responsables AS LR 
      WHERE LR.ResponsableKey = LSR.KEYRESPONSABLE
      )
    ;
    SET @iresponsables = @@ROWCOUNT;


    UPDATE LN
    SET
        LN.Niveau = LSN.NIVEAU
    FROM LISE.Niveaux AS LN
    INNER JOIN LISE.Staging_Niveaux AS LSN
      ON LN.NiveauID = LSN.IDNIVEAU
    WHERE ISNULL(LN.Niveau, '') <> ISNULL(LSN.NIVEAU, '')
    ;
    SET @univeaux = @@ROWCOUNT;

    INSERT INTO LISE.Niveaux (NiveauID, Niveau, EtablissementID)
    SELECT LSN.IDNIVEAU, LSN.NIVEAU, LSN.IDETABLISSEMENT
    FROM LISE.Staging_Niveaux AS LSN 
    WHERE NOT EXISTS (
      SELECT 1
      FROM LISE.Niveaux AS LN
      WHERE LN.NiveauID = LSN.IDNIVEAU
      )
    ;
    SET @iniveaux = @@ROWCOUNT;


    UPDATE LET
    SET
        LET.Etablissement = LSET.ETABLISSEMENT
    FROM LISE.Etablissements AS LET
    INNER JOIN LISE.Staging_Etablissements AS LSET
      ON LET.EtablissementID = LSET.IDETABLISSEMENT
    WHERE ISNULL(LET.Etablissement, '') <> ISNULL(LSET.ETABLISSEMENT, '')
    ;
    SET @uetablissements = @@ROWCOUNT;

    INSERT INTO LISE.Etablissements (EtablissementID, Etablissement)
    SELECT LSET.IDETABLISSEMENT, LSET.ETABLISSEMENT
    FROM LISE.Staging_Etablissements AS LSET
    WHERE NOT EXISTS (
      SELECT 1
      FROM LISE.Etablissements AS LET
      WHERE LET.EtablissementID = LSET.IDETABLISSEMENT
      )
    ;
    SET @ietablissements = @@ROWCOUNT;


    UPDATE LREG
    SET
        LREG.Regime = LSREG.REGIME
    FROM LISE.Regimes AS LREG
    INNER JOIN LISE.Staging_Regimes AS LSREG
      ON LREG.RegimeID = LSREG.IDREGIME
    WHERE ISNULL(LREG.Regime, '') <> ISNULL(LSREG.REGIME, '')
    ;
    SET @uregimes = @@ROWCOUNT;

    INSERT INTO LISE.Regimes (RegimeID, Regime)
    SELECT LSREG.IDREGIME, LSREG.REGIME
    FROM LISE.Staging_Regimes AS LSREG
    WHERE NOT EXISTS (
      SELECT 1
      FROM LISE.Regimes AS LREG
      WHERE LREG.RegimeID = LSREG.IDREGIME
      )
    ;
    SET @iregimes = @@ROWCOUNT;

    UPDATE LPA
    SET
      LPA.Nom = LSPA.NOM,
      LPA.Prenom = LSPA.PRENOM,
      LPA.FullName = LSPA.FULLNAME
    FROM LISE.Parents AS LPA 
    INNER JOIN LISE.Staging_Parents AS LSPA
      ON LPA.ResponsableID = LSPA.IDRESPONSABLE
    WHERE ISNULL(LPA.Nom, '') <> ISNULL(LSPA.NOM, '')
      OR ISNULL(LPA.Prenom, '') <> ISNULL(LSPA.PRENOM, '')
      OR ISNULL(LPA.FullName, '') <> ISNULL(LSPA.FULLNAME, '')
    ;
    SET @uparents = @@ROWCOUNT;

    INSERT INTO LISE.Parents (ResponsableID, Nom, Prenom, FullName)
    SELECT LSPA.IDRESPONSABLE, LSPA.NOM, LSPA.PRENOM, LSPA.FULLNAME
    FROM LISE.Staging_Parents AS LSPA
    WHERE NOT EXISTS (
      SELECT 1
      FROM LISE.Parents AS LPA
      WHERE LPA.ResponsableID = LSPA.IDRESPONSABLE
      )
    ;
    SET @iparents = @@ROWCOUNT;

    UPDATE LEN
    SET 
      LEN.Nom = LSEN.NOM,
      LEN.Prenom = LSEN.PRENOM,
      LEN.Sexe = LSEN.SEXE,
      LEN.DateNaissance = LSEN.DATENAISSANCE,
      LEN.Age = LSEN.AGE,
      LEN.Nationalite = LSEN.NATIONALITE,
      LEN.IdentiteNationale = LSEN.IDENTITENATIONALE,
      LEN.FullName = LSEN.FULLNAME
    FROM LISE.Enfants AS LEN
    INNER JOIN LISE.Staging_Enfants AS LSEN 
      ON LEN.EleveID = LSEN.IDELEVE
    WHERE ISNULL(LEN.Nom, '') <> ISNULL(LSEN.NOM, '')
      OR ISNULL(LEN.Prenom, '') <> ISNULL(LSEN.PRENOM, '')
      OR ISNULL(LEN.Sexe, '') <> ISNULL(LSEN.SEXE, '')
      OR ISNULL(LEN.DateNaissance, '') <> ISNULL(LSEN.DATENAISSANCE, '')
      OR ISNULL(LEN.Age, '') <> ISNULL(LSEN.AGE, '')
      OR ISNULL(LEN.Nationalite, '') <> ISNULL(LSEN.NATIONALITE, '')
      OR ISNULL(LEN.IdentiteNationale, '') <> ISNULL(LSEN.IDENTITENATIONALE, '')
      OR ISNULL(LEN.FullName, '') <> ISNULL(LSEN.FULLNAME, '')
    ;
    SET @uenfants = @@ROWCOUNT;

    INSERT INTO LISE.Enfants (EleveID, Nom, Prenom, Sexe, DateNaissance, Age, Nationalite, IdentiteNationale, FullName)
    SELECT LSEN.IDELEVE, LSEN.NOM, LSEN.PRENOM, LSEN.SEXE, LSEN.DATENAISSANCE, LSEN.AGE, LSEN.NATIONALITE, LSEN.IDENTITENATIONALE, LSEN.FULLNAME
    FROM LISE.Staging_Enfants AS LSEN
    WHERE NOT EXISTS (
      SELECT 1
      FROM LISE.Enfants AS LEN 
      WHERE LEN.EleveID = LSEN.IDELEVE
      )
    ;
    SET @ienfants = @@ROWCOUNT;


    UPDATE LF 
    SET
      LF.Ville = LSF.VILLE
    FROM LISE.Foyers AS LF
    INNER JOIN LISE.Staging_Foyers AS LSF
      ON LF.FoyerID = LSF.IDFOYER
    WHERE ISNULL(LF.Ville, '') <> ISNULL(LSF.VILLE, '')
    ;
    SET @ufoyers = @@ROWCOUNT;

    INSERT INTO LISE.Foyers (FoyerID, Ville, VilleID)
    SELECT LSF.IDFOYER, LSF.VILLE, LSF.IDVILLE
    FROM LISE.Staging_Foyers AS LSF
    WHERE NOT EXISTS (
      SELECT 1
      FROM LISE.Foyers AS LF
      WHERE LF.FoyerID = LSF.IDFOYER)
    ;
    SET @ifoyers = @@ROWCOUNT;


    UPDATE LS 
    SET
      LS.Service = LSS.SERVICE
    FROM LISE.Services AS LS 
    INNER JOIN LISE.Staging_Services AS LSS
      ON LS.ServiceID = LSS.IDSERVICE
    WHERE ISNULL(LS.Service, '') <> ISNULL(LSS.SERVICE, '')
    ;
    SET @uservices = @@ROWCOUNT;

    INSERT INTO LISE.Services (ServiceID, Service)
    SELECT LSS.IDSERVICE, LSS.SERVICE
    FROM LISE.Staging_Services AS LSS
    WHERE NOT EXISTS (
      SELECT 1
      FROM LISE.Services AS LS
      WHERE LS.ServiceID = LSS.IDSERVICE)
    ;
    SET @iservices = @@ROWCOUNT;

    
    UPDATE LV 
    SET
      LV.Ville = LSV.VILLE,
      LV.CodePostal = LSV.CODEPOSTAL,
      LV.Latitude = LSV.LATITUDE,
      LV.Longitude = LSV.LONGITUDE,
      LV.Departement = LSV.DEPARTEMENT,
      LV.Pays = LSV.PAYS
    FROM LISE.Villes AS LV
    INNER JOIN LISE.Staging_Villes AS LSV
      ON LV.VilleID = LSV.IDVILLE
    WHERE ISNULL(LV.Ville, '') <> ISNULL(LSV.VILLE, '')
      OR ISNULL(LV.CodePostal, '') <> ISNULL(LSV.CODEPOSTAL, '')
      OR ISNULL(LV.Latitude, '') <> ISNULL(LSV.LATITUDE, '')
      OR ISNULL(LV.Longitude, '') <> ISNULL(LSV.LONGITUDE, '')
      OR ISNULL(LV.Departement, '') <> ISNULL(LSV.DEPARTEMENT, '')
      OR ISNULL(LV.Pays, '') <> ISNULL(LSV.PAYS, '')
    ;
    SET @uvilles = @@ROWCOUNT;
    
    INSERT INTO LISE.Villes (VilleID, Ville, CodePostal, Latitude, Longitude, Departement, Pays)
    SELECT LSV.IDVILLE, LSV.VILLE, LSV.CODEPOSTAL, LSV.LATITUDE, LSV.LONGITUDE, LSV.DEPARTEMENT, LSV.PAYS
    FROM LISE.Staging_Villes AS LSV
    WHERE NOT EXISTS (
      SELECT 1
      FROM LISE.Villes AS LV
      WHERE LV.VilleID = LSV.IDVILLE)
    ;
    SET @ivilles = @@ROWCOUNT;

    UPDATE LY
    SET 
      LY.SchoolYearLibelle = LSY.SCHOOLYEARLIBELLE
    FROM LISE.SchoolYears AS LY 
    INNER JOIN LISE.Staging_SchoolYears AS LSY
      ON LY.SchoolYear = LSY.SCHOOLYEAR
    WHERE ISNULL(LY.SchoolYearLibelle, '') <> ISNULL(LSY.SCHOOLYEARLIBELLE, '')
    ;
    SET @uschoolyear = @@ROWCOUNT;

    INSERT INTO LISE.SchoolYears (SchoolYear, SchoolYearLibelle)
    SELECT LSY.SCHOOLYEAR, LSY.SCHOOLYEARLIBELLE
    FROM LISE.Staging_SchoolYears AS LSY 
    WHERE NOT EXISTS (
    SELECT 1
    FROM LISE.SchoolYears AS LY 
    WHERE LY.SchoolYear = LSY.SCHOOLYEAR)
    ;
    SET @ischoolyear = @@ROWCOUNT;
    
    INSERT INTO LISE.FacturesValidations (ValidationKey, ValidationID, TypeFacture, NombreFacture, DateValidation)
    SELECT LSFV.KEYVALIDATION, LSFV.IDVALIDATION, LSFV.TYPEFACTURE, LSFV.NOMBREFACTURE, LSFV.DATEVALIDATION
    FROM LISE.Staging_FacturesValidations AS LSFV
    WHERE LSFV.DATEVALIDATION IS NOT NULL
    AND NOT EXISTS (
      SELECT 1
      FROM LISE.FacturesValidations AS LFV
      WHERE LFV.ValidationKey = LSFV.KEYVALIDATION
        AND LFV.ValidationID = LSFV.IDVALIDATION 
        AND LFV.DateValidation  = LSFV.DATEVALIDATION  
      )
      ;
      SET @ifac_validations = @@ROWCOUNT; 


    INSERT INTO LISE.FacturesEleves (EleveKey, EleveID, ResponsableKey, ResponsableID, ValidationKey, ValidationID, ClasseKey, ClasseID, RegimeID, TotalEleve, DateFacture)
    SELECT LSFE.KEYELEVE, LSFE.IDELEVE, LSFE.KEYRESPONSABLE, LSFE.IDRESPONSABLE, LSFE.KEYVALIDATION, LSFE.IDVALIDATION, LSFE.KEYCLASSE, LSFE.IDCLASSE, LSFE.IDREGIME, LSFE.TOTALELEVE, LSFE.DATEFACTURE
    FROM LISE.Staging_FacturesEleves AS LSFE
    WHERE LSFE.DATEFACTURE IS NOT NULL AND 
    NOT EXISTS (
      SELECT 1
      FROM LISE.FacturesEleves AS LFE
      WHERE LFE.EleveKey = LSFE.KEYELEVE
        AND LFE.EleveID = LSFE.IDELEVE
        AND LFE.ResponsableKey = LSFE.KEYRESPONSABLE
        AND LFE.ResponsableID = LSFE.IDRESPONSABLE
        AND LFE.ValidationKey = LSFE.KEYVALIDATION
        AND LFE.ValidationID = LSFE.IDVALIDATION
        AND LFE.ClasseKey = LSFE.KEYCLASSE
        AND LFE.ClasseID = LSFE.IDCLASSE
        AND LFE.RegimeID = LSFE.IDREGIME
        AND LFE.DateFacture = LSFE.DATEFACTURE
        )
      ;
      SET @ifac_eleves = @@ROWCOUNT;


    INSERT INTO LISE.FacturesFamilles (ResponsableKey, ResponsableID, ValidationKey, ValidationID, FoyerID, ProfessionID, TotalFamille, DateFacture)
    SELECT LSFF.KEYRESPONSABLE, LSFF.IDRESPONSABLE, LSFF.KEYVALIDATION, LSFF.IDVALIDATION, LSFF.IDFOYER, LSFF.IDPROFESSION, LSFF.TOTALFAMILLE, LSFF.DATEFACTURE
    FROM LISE.Staging_FacturesFamilles AS LSFF
    WHERE LSFF.DATEFACTURE IS NOT NULL 
    AND NOT EXISTS (
      SELECT 1
      FROM LISE.FacturesFamilles AS LFF
      WHERE LFF.ResponsableKey = LSFF.KEYRESPONSABLE
        AND LFF.ResponsableID = LSFF.IDRESPONSABLE
        AND LFF.ValidationKey = LSFF.KEYVALIDATION
        AND LFF.ValidationID = LSFF.IDVALIDATION
        AND LFF.DateFacture = LSFF.DATEFACTURE
      )
    ;
    SET @ifac_familles = @@ROWCOUNT;

    INSERT INTO LISE.FacturesNiveaux (NiveauID, ValidationKey, ValidationID, ResponsableKey, ResponsableID, TotalNiveau, DateFacture)
    SELECT LSFN.IDNIVEAU, LSFN.KEYVALIDATION, LSFN.IDVALIDATION, LSFN.KEYRESPONSABLE, LSFN.IDRESPONSABLE, LSFN.TOTALNIVEAU, LSFN.DATEFACTURE
    FROM LISE.Staging_FacturesNiveaux AS LSFN
    WHERE LSFN.DATEFACTURE IS NOT NULL 
    AND NOT EXISTS (
      SELECT 1
      FROM LISE.FacturesNiveaux AS LFN
      WHERE LFN.NiveauID = LSFN.IDNIVEAU
        AND LFN.ValidationKey = LSFN.KEYVALIDATION
        AND LFN.ValidationID = LSFN.IDVALIDATION
        AND LFN.ResponsableKey = LSFN.KEYRESPONSABLE
        AND LFN.ResponsableID = LSFN.IDRESPONSABLE
        AND LFN.DateFacture = LSFN.DATEFACTURE
      )
    ;  
    SET @ifac_niveaux = @@ROWCOUNT;  


  INSERT INTO LISE.FacturesServices (EleveKey, EleveID, ResponsableKey, ResponsableID, ValidationKey, ValidationID, ServiceID, Quantite, Prix, Remise, TotalService, DateFacture)
  SELECT LSFS.KEYELEVE, LSFS.IDELEVE, LSFS.KEYRESPONSABLE, LSFS.IDRESPONSABLE, LSFS.KEYVALIDATION, LSFS.IDVALIDATION, LSFS.IDSERVICE, LSFS.QUANTITE, LSFS.PRIX, LSFS.REMISE, LSFS.TOTALSERVICE, LSFS.DATEFACTURE
  FROM LISE.Staging_FacturesServices AS LSFS
  WHERE LSFS.DATEFACTURE IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM LISE.FacturesServices AS LFS
    WHERE LFS.EleveKey = LSFS.KEYELEVE
      AND LFS.EleveID = LSFS.IDELEVE
      AND LFS.ResponsableKey = LSFS.KEYRESPONSABLE
      AND LFS.ResponsableID = LSFS.IDRESPONSABLE
      AND LFS.ValidationKey = LSFS.KEYVALIDATION 
      AND LFS.ServiceID = LSFS.IDSERVICE  
      AND LFS.DateFacture = LSFS.DATEFACTURE 
    )
    ;
    SET @ifac_services = @@ROWCOUNT;


  INSERT INTO LISE.Dates (DateID, Date, CalendarYear, CalendarMonth, CalendarDay, MonthName, DayName, SchoolYear, IsSchoolPeriod)
  SELECT LSD.IDDATE, LSD.DATE, LSD.CALENDARYEAR, LSD.CALENDARMONTH, LSD.CALENDARDAY, LSD.MONTHNAME, LSD.DAYNAME, LSD.SCHOOLYEAR, LSD.ISSCHOOLPERIOD
  FROM LISE.Staging_Dates AS LSD
  WHERE NOT EXISTS (
    SELECT 1
    FROM LISE.Dates AS LD
    WHERE LD.Date = LSD.DATE
    )
  ;
  SET @idates = @@ROWCOUNT;

  SET @TotalRowsWritten =
    COALESCE(@uclasses,0) + COALESCE(@iclasses,0) + COALESCE(@utargets,0) + COALESCE(@itargets,0) +
    COALESCE(@ueleves,0) + COALESCE(@ieleves,0) + COALESCE(@uprofessions,0) + COALESCE(@iprofessions,0) +
    COALESCE(@upays,0) + COALESCE(@ipays,0) + COALESCE(@ustaff,0) + COALESCE(@istaff,0) +
    COALESCE(@upersonnels,0) + COALESCE(@ipersonnels,0) + COALESCE(@uprofesseurs,0) + COALESCE(@iprofesseurs,0) +
    COALESCE(@uresponsables,0) + COALESCE(@iresponsables,0) + COALESCE(@univeaux,0) + COALESCE(@iniveaux,0) +
    COALESCE(@uetablissements,0) + COALESCE(@ietablissements,0) + COALESCE(@uregimes,0) + COALESCE(@iregimes,0) +
    COALESCE(@uparents,0) + COALESCE(@iparents,0) + COALESCE(@uenfants,0) + COALESCE(@ienfants,0) +
    COALESCE(@ufoyers,0) + COALESCE(@ifoyers,0) + COALESCE(@uservices,0) + COALESCE(@iservices,0) +
    COALESCE(@uvilles,0) + COALESCE(@ivilles,0) + COALESCE(@uschoolyear,0) + COALESCE(@ischoolyear,0) +
    COALESCE(@idates,0) + COALESCE(@ifac_services,0) + COALESCE(@ifac_niveaux,0) + COALESCE(@ifac_eleves,0) +
    COALESCE(@ifac_familles,0) + COALESCE(@ifac_validations,0);

  SELECT
    RowsUpdated_Classes = @uclasses,
    RowsInserted_Classes = @iclasses,
    RowsUpdated_Targets = @utargets,
    RowsInserted_Targets = @itargets,
    RowsUpdated_Eleves = @ueleves,
    RowsInserted_Eleves = @ieleves,
    RowsUpdated_Professions = @uprofessions,
    RowsInserted_Professions = @iprofessions,
    RowsUpdated_Pays = @upays,
    RowsInserted_Pays = @ipays,
    RowsUpdated_Staff = @ustaff,
    RowsInserted_Staff = @istaff,
    RowsUpdated_Personnels = @upersonnels,
    RowsInserted_Personnels = @ipersonnels,
    RowsUpdated_Professeurs = @uprofesseurs,
    RowsInserted_Professeurs = @iprofesseurs,
    RowsUpdated_Responsables = @uresponsables,
    RowsInserted_Responsables= @iresponsables,
    RowsUpdated_Niveaux = @univeaux,
    RowsInserted_Niveaux = @iniveaux,
    RowsUpdated_Etablissements = @uetablissements,
    RowsInserted_Etablissements= @ietablissements,
    RowsUpdated_Regimes = @uregimes,
    RowsInserted_Regimes = @iregimes,
    RowsUpdated_Parents = @uparents,
    RowsInserted_Parents = @iparents,
    RowsUpdated_Enfants = @uenfants,
    RowsInserted_Enfants = @ienfants,
    RowsUpdated_Foyers = @ufoyers,
    RowsInserted_Foyers = @ifoyers,
    RowsUpdated_Services = @uservices,
    RowsInserted_Services = @iservices,
    RowsUpdated_Villes = @uvilles,
    RowsInserted_Villes = @ivilles,
    RowsUpdated_SchoolYear = @uschoolyear,
    RowsInserted_SchoolYear = @ischoolyear,
    RowsInserted_Dates = @idates,
    RowsInserted_Fac_Services = @ifac_services,
    RowsInserted_Fac_Niveaux = @ifac_niveaux,
    RowsInserted_Fac_Eleves = @ifac_eleves,
    RowsInserted_Fac_Familles = @ifac_familles,
    RowsInserted_Fac_Validations = @ifac_validations,
    TotalRowsWritten = @TotalRowsWritten;
END