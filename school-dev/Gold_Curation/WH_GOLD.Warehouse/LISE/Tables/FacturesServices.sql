CREATE TABLE [LISE].[FacturesServices] (

	[EleveKey] varchar(50) NULL, 
	[EleveID] int NULL, 
	[ResponsableKey] varchar(50) NULL, 
	[ResponsableID] int NULL, 
	[ValidationKey] varchar(50) NULL, 
	[ValidationID] int NULL, 
	[ServiceID] int NULL, 
	[Quantite] float NULL, 
	[Prix] float NULL, 
	[Remise] float NULL, 
	[TotalService] float NULL, 
	[DateFacture] date NULL
);