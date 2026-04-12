-- Auto Generated (Do not modify) 9243BB267AE1247067DA37C1B728D403DDA671C295B6D42840BB55DDEEC3CDF3
CREATE   VIEW LISE.vw_RevenueServicesDetailed
AS
WITH FE_Map AS (
    SELECT DISTINCT
        EleveKey,
        EleveID,
        ResponsableKey,
        ResponsableID,
        ValidationKey,
        ValidationID,
        ClasseID
    FROM LISE.FacturesEleves
)
SELECT
    FS.EleveKey,
    FS.EleveID,
    FS.ResponsableKey,
    FS.ResponsableID,
    FS.ValidationKey,
    FS.ValidationID,
    FS.ServiceID,
    FS.Quantite,
    FS.Prix,
    FS.Remise,
    FS.TotalService,
    FS.DateFacture,
    D.CalendarYear,
    D.CalendarMonth,
    D.MonthName,
    D.SchoolYear,
    S.Service,
    FE.ClasseID,
    C.Classe,
    C.ClasseLibelle,
    C.NiveauID,
    N.Niveau,
    C.EtablissementID,
    ET.Etablissement
FROM LISE.FacturesServices AS FS
LEFT JOIN LISE.Dates AS D
    ON FS.DateFacture = D.[Date]
LEFT JOIN LISE.Services AS S
    ON FS.ServiceID = S.ServiceID
LEFT JOIN FE_Map AS FE
    ON FS.EleveKey = FE.EleveKey
   AND FS.EleveID = FE.EleveID
   AND FS.ResponsableKey = FE.ResponsableKey
   AND FS.ResponsableID = FE.ResponsableID
   AND FS.ValidationKey = FE.ValidationKey
   AND FS.ValidationID = FE.ValidationID
LEFT JOIN LISE.Classes AS C
    ON FE.ClasseID = C.ClasseID
LEFT JOIN LISE.Niveaux AS N
    ON C.NiveauID = N.NiveauID
LEFT JOIN LISE.Etablissements AS ET
    ON C.EtablissementID = ET.EtablissementID;