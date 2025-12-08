[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dropzone_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "PersonnelID", DestinationColumnName = "PersonnelID"], [SourceColumnName = "Nom", DestinationColumnName = "Nom"], [SourceColumnName = "Prenom", DestinationColumnName = "Prenom"], [SourceColumnName = "Periode", DestinationColumnName = "Periode"], [SourceColumnName = "Date", DestinationColumnName = "Date"], [SourceColumnName = "Montant", DestinationColumnName = "Montant"], [SourceColumnName = "Devise", DestinationColumnName = "Devise"], [SourceColumnName = "ProfessionTypeID", DestinationColumnName = "ProfessionTypeID"], [SourceColumnName = "EtablissementID", DestinationColumnName = "EtablissementID"], [SourceColumnName = "ClasseID", DestinationColumnName = "ClasseID"], [SourceColumnName = "NiveauID", DestinationColumnName = "NiveauID"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared dropzone = let
  // --- navigation inchangée ---
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = WorkspaceId]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = LakehouseId]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "Payroll"]}[Content],
  Dropzone = #"Navigation 3"{[Name = "dropzone"]}[Content],


  // --- sélection du fichier paramétré ---
  TargetFile = "Payroll_" & pPayPeriod & ".csv",
  ThisFile = Table.SelectRows(Dropzone, each Text.Lower([Name]) = Text.Lower(TargetFile)),
  AssertOne = if Table.RowCount(ThisFile) <> 1
              then error "Fichier introuvable dans dropzone : " & TargetFile
              else ThisFile,


  // --- lecture CSV ---
  FileBin  = AssertOne{0}[Content],
  Csv      = Csv.Document(FileBin, [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
  Promote  = Table.PromoteHeaders(Csv, [PromoteAllScalars=true]),
  TrimCols = Table.TransformColumnNames(Promote, each Text.Trim(_)),


  // --- typer sans renommer les colonnes FR ---
  // Montant en décimale FR + Date en texte pour parse yyyyMMdd
  Types1 = Table.TransformColumnTypes(TrimCols,{
      {"PersonnelID", type text},
      {"Nom", type text},
      {"Prenom", type text},
      {"Periode", type text},
      {"Date", type text},       // reste texte pour parser ensuite
      {"Montant", type text},    // on convertira avec culture/fr
      {"Devise", type text},
      {"ProfessionTypeID", Int64.Type},
      {"EtablissementID", Int64.Type},
      {"ClasseID", Int64.Type},
      {"NiveauID", Int64.Type}
  }),


  // --- parse Date : yyyyMMdd -> date ---
  DatePadded = Table.TransformColumns(Types1, {{"Date", each Text.PadStart(Text.From(_), 8, "0"), type text}}),
  WithDate = Table.AddColumn(DatePadded, "DateParsee", each
              let y = Text.Start([Date], 4),
                  m = Text.Middle([Date], 4, 2),
                  d = Text.End([Date], 2)
              in Date.From(y & "-" & m & "-" & d), type date),


  // --- PeriodeCle 'YYYY-MM' dérivée de DateParsee ---
  WithPeriodeCle = Table.AddColumn(WithDate, "PeriodeCle", each
                     let y = Text.From(Date.Year([DateParsee])),
                         m = Text.PadStart(Text.From(Date.Month([DateParsee])), 2, "0")
                     in y & "-" & m, type text),


  // --- Montant : convertir "1 747,52" -> 1747.52 ---
  // on retire les espaces (y compris insécables) et remplace ',' par '.'
  CleanMontant =
    Table.TransformColumns(
        WithPeriodeCle,
        {
            { "Montant",
              each
                let t = Text.Trim(Text.From(_))
                in if t = null or t = "" then null else Number.FromText(t, "fr-FR"),
              type number
            }
        }
    ),


  // --- normaliser Devise (facultatif) ---
  DeviseTrim = Table.TransformColumns(CleanMontant, {{"Devise", each Text.Trim(_), type text}}),


  // --- sortir les colonnes dans l'ordre de dbo.Paie_Incoming, sans renommer ---
  Reordered = Table.SelectColumns(DeviseTrim, {
      "PersonnelID","Nom","Prenom","Periode",
      "DateParsee","PeriodeCle",
      "Montant","Devise","ProfessionTypeID","EtablissementID","ClasseID","NiveauID"
  }),
  RenamedFinal = Table.RenameColumns(Reordered, {{"DateParsee","Date"}}),
  TypedFinal = Table.TransformColumnTypes(RenamedFinal, {{"Date", type date}, {"PeriodeCle", type text}, {"Montant", type number}, {"PersonnelID", Int64.Type}}),
  #"Replaced value" = Table.ReplaceValue(TypedFinal, null, 0, Replacer.ReplaceValue, {"Montant"})
in
  #"Replaced value";
shared pPayPeriod = "2025-12" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text, DefaultValue = ""];
shared dropzone_DataDestination = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = null, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "28e6a84a-1953-410e-8b52-272e6318afde"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "d86bd324-6717-4394-a68f-bcb9a744a991"]}[Data],
  TableNavigation = Navigation_2{[Schema = "LISE", Item = "Payroll_Incoming"]}[Data]
in
  TableNavigation;
shared WorkspaceId = "28e6a84a-1953-410e-8b52-272e6318afde" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type any, DefaultValue = ""];
shared LakehouseId = "bfe479b8-2f70-44bc-84d5-dfa2ec50d321" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type any, DefaultValue = ""];
