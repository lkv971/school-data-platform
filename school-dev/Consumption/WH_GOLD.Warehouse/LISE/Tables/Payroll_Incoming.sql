CREATE TABLE [LISE].[Payroll_Incoming] (

	[PersonnelID] int NULL, 
	[Nom] varchar(100) NULL, 
	[Prenom] varchar(100) NULL, 
	[Periode] varchar(50) NULL, 
	[Date] date NULL, 
	[Montant] float NULL, 
	[Devise] varchar(20) NULL, 
	[ProfessionTypeID] int NULL, 
	[EtablissementID] int NULL, 
	[ClasseID] int NULL, 
	[NiveauID] int NULL
);