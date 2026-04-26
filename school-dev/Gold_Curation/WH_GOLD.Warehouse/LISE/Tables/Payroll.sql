CREATE TABLE [LISE].[Payroll] (

	[PaieID] uniqueidentifier NOT NULL, 
	[PersonnelID] int NOT NULL, 
	[Nom] varchar(255) NULL, 
	[Prenom] varchar(255) NULL, 
	[Periode] varchar(100) NOT NULL, 
	[Date] date NOT NULL, 
	[Montant] decimal(18,2) NOT NULL, 
	[Devise] varchar(20) NULL, 
	[ProfessionTypeID] int NOT NULL, 
	[EtablissementID] int NOT NULL, 
	[ClasseID] int NULL, 
	[NiveauID] int NULL
);