-- Auto Generated (Do not modify) C9C9435B3DA5C018B352661C3FC4956C58E3A48CAE1310806938F58E79AC3324
create view "dbt_school_dev"."stg_lise__payroll" as select
    PaieID,
    PersonnelID,
    Nom,
    Prenom,
    Periode,
    Date,
    Montant,
    Devise,
    ProfessionTypeID,
    EtablissementID,
    ClasseID,
    NiveauID
from "WH_GOLD"."LISE"."Payroll";