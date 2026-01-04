CREATE TABLE [LISE].[Dates] (

	[DateID] int NOT NULL, 
	[Date] date NOT NULL, 
	[CalendarYear] int NULL, 
	[CalendarMonth] int NULL, 
	[CalendarDay] int NULL, 
	[MonthName] varchar(100) NULL, 
	[DayName] varchar(100) NULL, 
	[SchoolYear] char(9) NULL, 
	[IsSchoolPeriod] int NULL
);