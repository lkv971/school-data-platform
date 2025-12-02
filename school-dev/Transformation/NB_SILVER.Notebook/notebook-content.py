# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b727fb41-33d0-41ec-90bd-dfc3c112f2b3",
# META       "default_lakehouse_name": "LH_SILVER",
# META       "default_lakehouse_workspace_id": "28e6a84a-1953-410e-8b52-272e6318afde",
# META       "known_lakehouses": [
# META         {
# META           "id": "bfe479b8-2f70-44bc-84d5-dfa2ec50d321"
# META         },
# META         {
# META           "id": "b727fb41-33d0-41ec-90bd-dfc3c112f2b3"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
from delta.tables import DeltaTable
from functools import reduce
from operator import or_
from datetime import datetime, timezone
from notebookutils import mssparkutils
from zoneinfo import ZoneInfo 
import uuid
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

BRONZE_BASE = "abfss://LISE@onelake.dfs.fabric.microsoft.com/LH_BRONZE.Lakehouse/Files"

def p(year: str, filename: str) -> str:
    return f"{BRONZE_BASE}/Lise_Data/{year}/{filename}"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

paths = {
    "2023-2024": {
        "niveaux":             p("2023-2024", "COM_NIVEAU_2324.csv"),
        "etablissements":      p("2023-2024", "COM_ETABLISSEMENT_2324.csv"),
        "classes":             p("2023-2024", "COM_CLASSES_2324.csv"),
        "foyers":              p("2023-2024", "COM_FOYER_2324.csv"),
        "responsables":        p("2023-2024", "COM_RESPONSABLES_2324.csv"),
        "professions":         p("2023-2024", "TAB_CSP_2324.csv"),
        "eleves":              p("2023-2024", "COM_ELEVES_2324.csv"),
        "factures_niveaux":    p("2023-2024", "FAC_COMPTA_GENERAL_2324.csv"),
        "factures_services":   p("2023-2024", "FAC_HISTO_LIGNE_2324.csv"),
        "factures_familles":   p("2023-2024", "FAC_HISTO_FAMILLE_2324.csv"),
        "factures_eleves":     p("2023-2024", "FAC_HISTO_ELEVE_2324.csv"),
        "factures_validations":p("2023-2024", "FAC_VALIDATION_2324.csv"),
        "personnels":          p("2023-2024", "COM_PERSONNELS_2324.csv"),
        "professeurs":         p("2023-2024", "COM_PROFS_PRINCIPAUX_2324.csv"),
        "pays":                p("2023-2024", "TAB_PAYS_2324.csv"),
    },
    "2024-2025": {
        "niveaux":             p("2024-2025", "COM_NIVEAU_2425.csv"),
        "etablissements":      p("2024-2025", "COM_ETABLISSEMENT_2425.csv"),
        "classes":             p("2024-2025", "COM_CLASSES_2425.csv"),
        "foyers":              p("2024-2025", "COM_FOYER_2425.csv"),
        "responsables":        p("2024-2025", "COM_RESPONSABLES_2425.csv"),
        "professions":         p("2024-2025", "TAB_CSP_2425.csv"),
        "eleves":              p("2024-2025", "COM_ELEVES_2425.csv"),
        "factures_niveaux":    p("2024-2025", "FAC_COMPTA_GENERAL_2425.csv"),
        "factures_services":   p("2024-2025", "FAC_HISTO_LIGNE_2425.csv"),
        "factures_familles":   p("2024-2025", "FAC_HISTO_FAMILLE_2425.csv"),
        "factures_eleves":     p("2024-2025", "FAC_HISTO_ELEVE_2425.csv"),
        "factures_validations":p("2024-2025", "FAC_VALIDATION_2425.csv"),
        "personnels":          p("2024-2025", "COM_PERSONNELS_2425.csv"),
        "professeurs":         p("2024-2025", "COM_PROFS_PRINCIPAUX_2425.csv"),
        "pays":                p("2024-2025", "TAB_PAYS_2425.csv"),
    },
    "2025-2026": {
        "niveaux":             p("2025-2026", "COM_NIVEAU.csv"),
        "etablissements":      p("2025-2026", "COM_ETABLISSEMENT.csv"),
        "classes":             p("2025-2026", "COM_CLASSES.csv"),
        "foyers":              p("2025-2026", "COM_FOYER.csv"),
        "responsables":        p("2025-2026", "COM_RESPONSABLES.csv"),
        "professions":         p("2025-2026", "TAB_CSP.csv"),
        "eleves":              p("2025-2026", "COM_ELEVES.csv"),
        "factures_niveaux":    p("2025-2026", "FAC_COMPTA_GENERAL.csv"),
        "factures_services":   p("2025-2026", "FAC_HISTO_LIGNE.csv"),
        "factures_familles":   p("2025-2026", "FAC_HISTO_FAMILLE.csv"),
        "factures_eleves":     p("2025-2026", "FAC_HISTO_ELEVE.csv"),
        "factures_validations":p("2025-2026", "FAC_VALIDATION.csv"),
        "personnels":          p("2025-2026", "COM_PERSONNELS.csv"),
        "professeurs":         p("2025-2026", "COM_PROFS_PRINCIPAUX.csv"),
        "pays":                p("2025-2026", "TAB_PAYS.csv"),
    }
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_csv(year: str, dataset_name: str):
    try:
        path = paths[year][dataset_name]
        df = spark.read.csv(path, header=True, sep=",", quote='"', escape='"', encoding="UTF-16LE", multiLine=True)
        
        clean_columns = [c.strip().replace("\uFEFF", "").replace('"', "").strip() for c in df.columns]
        df = df.toDF(*clean_columns) 

        for col_name in df.columns:
            df = df.withColumn(
                col_name,
                regexp_replace(col(col_name), "[\uFEFF\x00-\x1F\x7F]", ""))
               
        df = df.withColumn("SCHOOLYEAR", lit(year)) \

        print(f"Table at {path} read successfully")
        return df

    except Exception as e:
        print(f"Error reading at {path}:{e}")
        raise

def union_dfs(dataset_name):
    dfs = [read_csv(year, dataset_name) for year in paths]
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs) if dfs else None
    

df_niveaux = union_dfs("niveaux")
df_etablissements = union_dfs("etablissements")
df_classes = union_dfs("classes")
df_foyers = union_dfs("foyers")
df_responsables = union_dfs("responsables")
df_professions = union_dfs("professions")
df_ecoliers = union_dfs("eleves")
df_factures_niveaux = union_dfs("factures_niveaux")
df_factures_services = union_dfs("factures_services")
df_factures_familles = union_dfs("factures_familles")
df_factures_eleves = union_dfs("factures_eleves")
df_factures_validations = union_dfs("factures_validations")
df_personnels = union_dfs("personnels")
df_professeurs = union_dfs("professeurs")
df_pays = union_dfs("pays")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

new_class_rows = [
    Row(IDCLASSE=24, CL_CODE="5EG", CL_LIBELLE="5ème - Gamma", IDETABLISSEMENT=1, IDNIVEAU=3, CL_CLASSE_RECTORAT="5EME", SCHOOLYEAR="2024-2025"),
    Row(IDCLASSE=25, CL_CODE="5EK", CL_LIBELLE="5ème - Kappa", IDETABLISSEMENT=1, IDNIVEAU=3, CL_CLASSE_RECTORAT="5EME", SCHOOLYEAR="2024-2025"),
    Row(IDCLASSE=26, CL_CODE="6E", CL_LIBELLE="6ème", IDETABLISSEMENT=1, IDNIVEAU=3, CL_CLASSE_RECTORAT="6EME", SCHOOLYEAR="2024-2025")
]

df_classes = df_classes.select("IDCLASSE", "CL_CODE", "CL_LIBELLE", "IDETABLISSEMENT", "IDNIVEAU", "CL_CLASSE_RECTORAT", "SCHOOLYEAR")

df_new_class_rows = spark.createDataFrame(new_class_rows, schema = df_classes.schema)

df_classes = df_classes.unionByName(df_new_class_rows)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

villes_schema = StructType([
    StructField("VILLE", StringType(), True),
    StructField("CODEPOSTAL", IntegerType(), True),
    StructField("LATITUDE", DoubleType(), True),
    StructField("LONGITUDE", DoubleType(), True),
    StructField("DEPARTEMENT", StringType(), True),
    StructField("PAYS", StringType(), True)
])

villes_path = "abfss://LISE@onelake.dfs.fabric.microsoft.com/LH_BRONZE.Lakehouse/Files/External_Data/VILLES.csv"

window_villes = Window.orderBy(col("VILLE"))

df_villes = spark.read.csv(villes_path, schema= villes_schema, header = True, sep = ",", encoding="UTF-8") 

df_villes = df_villes.withColumn("IDVILLE", row_number().over(window_villes)) \
                     .filter(col("DEPARTEMENT") == "GUADELOUPE") \
                     .select("IDVILLE", "VILLE", "CODEPOSTAL", "LATITUDE", "LONGITUDE", "DEPARTEMENT", "PAYS")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

service_schema = StructType([
    StructField("IDSERVICE", IntegerType(), True),
    StructField("SERVICE", StringType(), True)
])

service_data = [
    (1, "SCOLARITE"),
    (2, "CANTINE"),
    (3, "ETUDE"),
    (4, "GARDERIE"),
    (5, "VOYAGE"),
    (6, "UNIFORME"),
    (7, "SORTIE"),
    (8, "PSG"),
    (9, "FRAIS"),
    (10, "CAMBRIDGE"),
    (11, "BABY LISE"),
    (12, "OUTDOOR"),
    (13, "FOURNITURE")]

df_services = spark.createDataFrame(service_data, schema = service_schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

regimes_schema = StructType([
    StructField("IDREGIME", IntegerType(), True),
    StructField("REGIME", StringType(), True)
])

regimes_data = [
    (1, "DEMI-PENSIONNAIRE"),
    (2, "EXTERNE")
]

df_regimes = spark.createDataFrame(regimes_data, schema = regimes_schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_etablissements = df_etablissements.withColumn("IDETABLISSEMENT", col("IDETABLISSEMENT").cast("int")) \
                                     .filter(col("IDETABLISSEMENT") != 3) \
                                     .withColumn("ETABLISSEMENT", regexp_replace(col("ET_LIBELLE"), "L.I.S.E COLLEGE", "L.I.S.E PRIMARY"))\
                                     .select(col("IDETABLISSEMENT").cast(IntegerType()), 
                                             "ETABLISSEMENT")                              

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_niveaux = df_niveaux.withColumn("NIVEAU", 
                                   when(col("NI_CODE") == "MAT", "MATERNELLE")
                                   .when(col("NI_CODE") == "PRIM", "PRIMAIRE")
                                   .when(col("NI_CODE") == "AE", "ACTIVITES EXTRASCOLAIRES")
                                   .when(col("NI_CODE") == "6E 5E 4E 3E", "COLLEGE")  
                                   .otherwise(col("NI_CODE"))) \
                        .withColumn("IDNIVEAU", 
                                   when(col("NIVEAU") == "MATERNELLE", 1)
                                   .when(col("NIVEAU") == "PRIMAIRE", 2)
                                   .when(col("NIVEAU") == "COLLEGE", 3)
                                   .when(col("NIVEAU") == "ACTIVITES EXTRASCOLAIRES", 4)) \
                        .withColumn("IDETABLISSEMENT", 
                                   when(col("NIVEAU").isin("MATERNELLE", "COLLEGE", "ACTIVITES EXTRASCOLAIRES"), 1) 
                                   .otherwise(2)
                                  )\
                        .select(col("IDNIVEAU").cast(IntegerType()), 
                                "NIVEAU",
                                col("IDETABLISSEMENT").cast(IntegerType()))                              

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

classes_primaire = ["CP", "CE1", "CE2", "CM1", "CM2"]
classes_maternelle = ["TP", "PS", "MS", "GS"]
classes_college = ["6EME", "5EME", "4EME", "3EME"]
id_niveaux_academy = [9, 1, 3]
old_classe_keys2425 =["2024-2025-9", "2024-2025-11"]


df_classes = df_classes.withColumnRenamed("CL_CLASSE_RECTORAT", "CLASSE") \
                       .withColumn("CLASSE", regexp_replace(col("CLASSE"), "6ÈME", "6EME")) \
                       .withColumn("CLASSELIBELLE", split(col("CL_LIBELLE"), "-").getItem(1)) \
                       .withColumn("IDNIVEAU", when(col("CLASSE").isin(classes_primaire), 2)
                                               .when(col("CLASSE").isin(classes_maternelle), 1)
                                               .when(col("CLASSE").isin(classes_college), 3)
                                               .otherwise(4)) \
                       .withColumn("IDETABLISSEMENT", when(col("IDNIVEAU").isin(id_niveaux_academy), 1).otherwise(2)) \
                       .withColumn("CLASSE", when(col("IDCLASSE") == 20, "AE").otherwise(col("CLASSE"))) \
                       .withColumn("CLASSELIBELLE", when(col("IDCLASSE") == 20, "Activités ExtraScolaires").otherwise(col("CLASSELIBELLE"))) \
                       .withColumn("CLASSELIBELLE", coalesce(col("CLASSELIBELLE"), col("CLASSE"))) \
                       .withColumn("CLASSELIBELLE", when(col("CLASSELIBELLE").isin(classes_college), lower(col("CLASSELIBELLE"))).otherwise(col("CLASSELIBELLE")))\
                       .withColumn("IDETABLISSEMENT", when(col("IDCLASSE") == 20, 1).otherwise(col("IDETABLISSEMENT")))


df_classes_targets = df_classes.withColumn("TARGETCOUNT", when(col("CLASSE")== "3EME", 10).otherwise(20)) \
                              .withColumn("CLASSELIBELLE", coalesce(col("CLASSELIBELLE"), col("CLASSE"))) \
                              .withColumn("TARGETCOUNT", when(col("CLASSE") == "AE", lit(None).cast(IntegerType())).otherwise(col("TARGETCOUNT"))) \
                              .withColumn("MAXIMUMCOUNT", when(col("CLASSE") == "AE", lit(None).cast(IntegerType())).otherwise(22)) \
                              .withColumn("KEYCLASSE", concat(col("SCHOOLYEAR"),
                                                              lit("-"),
                                                              col("IDCLASSE"))) \
                              .filter(~col("KEYCLASSE").isin(old_classe_keys2425))


df_classes_targets = df_classes_targets.select("KEYCLASSE",
                                               col("IDCLASSE").cast(IntegerType()),
                                               col("TARGETCOUNT").cast(IntegerType()),
                                               col("MAXIMUMCOUNT").cast(IntegerType()),
                                               "SCHOOLYEAR")  

df_classes = df_classes.select(col("IDCLASSE").cast(IntegerType()), 
                               "CLASSE", 
                               "CLASSELIBELLE",
                               col("IDNIVEAU").cast(IntegerType()),
                               col("IDETABLISSEMENT").cast(IntegerType()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

communes_gwada = [
    "LES ABYMES",
    "ANSE BERTRAND",
    "BAIE MAHAULT",
    "BAILLIF",
    "BASSE TERRE",
    "BOUILLANTE",
    "CAPESTERRE BELLE EAU",
    "CAPESTERRE DE MARIE GALANTE",
    "DESHAIES",
    "LA DESIRADE",
    "LE GOSIER",
    "GOURBEYRE",
    "GOYAVE",
    "GRAND BOURG",
    "HORS GUADELOUPE",
    "LAMENTIN",
    "MORNE A L EAU",
    "LE MOULE",
    "PETIT BOURG",
    "PETIT CANAL",
    "POINTE A PITRE",
    "POINTE NOIRE",
    "PORT LOUIS",
    "SAINT CLAUDE",
    "SAINT FRANCOIS",
    "SAINT LOUIS",
    "SAINTE ANNE",
    "SAINTE ROSE",
    "TERRE DE BAS",
    "TERRE DE HAUT",
    "TROIS RIVIERES",
    "VIEUX FORT",
    "VIEUX HABITANTS"
]

df_foyers = df_foyers.withColumn("VILLE", regexp_replace(col("VILLE"), "STE ", "SAINTE ")) \
                     .withColumn("VILLE", regexp_replace(col("VILLE"), "ST ", "SAINT ")) \
                     .withColumn("VILLE", regexp_replace(col("VILLE"), "Baie-Mahault", "BAIE MAHAULT")) \
                     .withColumn("VILLE", regexp_replace(col("VILLE"), "BAIE-", "BAIE MAHAULT")) \
                     .withColumn("VILLE", regexp_replace(col("VILLE"), "BAIE MAHAULTMAHAULT", "BAIE MAHAULT")) \
                     .withColumn("VILLE", regexp_replace(col("VILLE"), "-", " ")) \
                     .withColumn("VILLE", when(~col("VILLE").isin(communes_gwada), "HORS GUADELOUPE").otherwise(col("VILLE"))) \
                     .withColumn("VILLE", upper(col("VILLE"))) \
                     .filter(col("VILLE").isNotNull() & (trim(col("VILLE")) != "")) 

df_foyers = df_foyers.join(broadcast(df_villes), on = "VILLE", how = "left") \
                     .withColumn("IDVILLE", when(col("IDVILLE").isNull(), 33).otherwise(col("IDVILLE"))) \
                     .withColumn("VILLE", when(col("VILLE").isNull(), "HORS GUADELOUPE").otherwise(col("VILLE"))) \
                     .select(col("IDFOYER").cast(IntegerType()), 
                             "VILLE",
                             col("IDVILLE").cast(IntegerType()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_professions = df_professions.withColumnRenamed("CSP_CODE", "IDPROFESSION")\
                               .withColumnRenamed("CSP_LIBELLE", "PROFESSION") \
                               .select(col("IDPROFESSION").cast(IntegerType()), 
                                       "PROFESSION")                           

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_pays = df_pays.withColumnRenamed("PA_CODE", "IDPAYS") \
                 .withColumnRenamed("PA_PAYS", "PAYS") \
                 .withColumnRenamed("PA_NATIONALITE", "NATIONALITE") \
                 .select(col("IDPAYS").cast(IntegerType()),
                         "PAYS",
                         "NATIONALITE")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def parse_date(colnames):
    return when(trim(col(colnames)).isin("NULL", "", "0", "NaN", "InvalidDate", "00000000"), lit(None)).otherwise(to_date(trim(col(colnames)), "yyyyMMdd"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

age_years = floor(months_between(current_date(), col("DATENAISSANCE")) / 12)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_personnels = df_personnels.withColumnRenamed("PE_NOM", "NOM") \
    .withColumnRenamed("PE_PRENOM", "PRENOM") \
    .withColumnRenamed("PE_TYPE", "TYPE") \
    .withColumnRenamed("PE_VILLE", "VILLE") \
    .withColumnRenamed("PE_PAYS", "PAYS") \
    .withColumnRenamed("PE_DATE_ENTREE", "DATEENTREE") \
    .withColumnRenamed("PE_DATE_SORTIE", "DATESORTIE") \
    .withColumnRenamed("PE_TELPORTABLE", "TELEPHONE") \
    .withColumnRenamed("PE_EMAIL_PRO", "EMAIL") \
    .withColumnRenamed("PE_NAISSANCE_DATE", "DATENAISSANCE") \
    .withColumnRenamed("PE_NUMSECU", "SECURITESOCIALE") \
    .withColumnRenamed("PE_BADGENUM", "BADGE") \
    .withColumnRenamed("PE_IBAN", "NUMEROCOMPTE") \
    .withColumn("NOM", when(col("IDPERSONNEL") == 18, "CLEDE" ).otherwise(col("NOM"))) \
    .withColumn("DATEENTREE", parse_date("DATEENTREE")) \
    .withColumn("DATESORTIE", parse_date("DATESORTIE")) \
    .withColumn("DATENAISSANCE", parse_date("DATENAISSANCE")) \
    .withColumn("TYPE", regexp_replace(col("TYPE"), "prof", "Enseignant")) \
    .withColumn("TYPE", regexp_replace(col("TYPE"), "exterieur", "Agent")) \
    .withColumn("TYPE", when(col("IDPERSONNEL").isin(70, 71), "Apprentie") 
                      .when(col("IDPERSONNEL").isin(17, 33), "Cadre")
                      .when(col("IDPERSONNEL").isin(22, 49), "Administration")
                      .otherwise(col("TYPE"))) \
    .withColumn("VILLE", regexp_replace(col("VILLE"), "STE ", "SAINTE ")) \
    .withColumn("VILLE", regexp_replace(col("VILLE"), "ST ", "SAINT ")) \
    .withColumn("VILLE", regexp_replace(col("VILLE"), "JARRY", "BAIE MAHAULT")) \
    .withColumn("VILLE", regexp_replace(col("VILLE"), "-", "")) \
    .withColumn("TELEPHONE", regexp_replace(col("TELEPHONE"), r"[\s-]", "")) \
    .withColumn("TELEPHONE", regexp_replace(col("TELEPHONE"), r"^\+(590|596|594|33)", "0")) \
    .withColumn("TELEPHONE", regexp_replace(col("TELEPHONE"), r"\?", "")) \
    .withColumn("EMAIL", lower(concat(substring(trim(col("PRENOM")), 1, 1), lit("."), 
                                      split(trim(col("NOM")), r"\s+").getItem(0),
                                      lit("@kudzaisolutions.com")))) \
    .withColumn("EMAIL", regexp_replace(col("EMAIL"), r"@.*$", "@kudzaisolutions.onmicrosoft.com")) \
    .withColumn("AGE", when(col("DATENAISSANCE").isNotNull() & age_years.between(0,120), age_years).otherwise(lit(None))) \
    .withColumn("KEYPERSONNEL", concat(col("SCHOOLYEAR"), lit("-"), col("IDPERSONNEL"))) 

df_staff = df_personnels.select(
    "KEYPERSONNEL",
    col("IDPERSONNEL").cast(IntegerType()),
    "VILLE",
    "DATEENTREE",
    "DATESORTIE",
    col("TELEPHONE"),  
    "EMAIL",
    "DATENAISSANCE",
    col("AGE").cast(IntegerType()))

df_personnels = df_personnels.join(df_pays, on=(df_pays["IDPAYS"] == df_personnels["PE_NATIONALITE"]), how="left") \
        .select(col("IDPERSONNEL").cast(IntegerType()),
        "NOM",
        "PRENOM",
        "NATIONALITE",     
        col("BADGE").cast(IntegerType()))

df_professeurs = df_professeurs.join(df_classes, on = "IDCLASSE", how = "left") \
                               .withColumnRenamed("IDPROFSPRINCIPAUX", "IDPROFESSEUR") \
                               .select(col("IDPROFESSEUR").cast(IntegerType()),
                                       col("IDPERSONNEL").cast(IntegerType()),
                                       col("IDCLASSE").cast(IntegerType()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


banques_populaires = ["%1010 7%", "%1020 7%", "%1080 7%", "%1090 7%", "%1130 7%", "%1190 7%", "%1350 7%", "%1360 7%", "%1380 7%",
"%1390 7%", "%1460 7%", "%1470 7%", "%1560 7%", "%1660 7%", "%1670 7%", "%1680 7%", "%1760 7%", "%1780 7%", "%1870 7%"]

credit_agricole = ["%1020 6%", "%1400 6%", "%1100 6%", "%1020 6 %", "%1100 6%", "%1107 6%", "%1120 6%", "%1130 6%", "%1120 6%",
"%1130 6%", "%1170 6%", "%1200 6%", "%1220 6%", "%1240 6%", "%1250 6%", "%1290 6%", "%1310 6%", "%1321 0%", "%1330 6%", "%1350 6%",
"%1360 6%", "%1390 6%", "%1440 6%", "%1450 6%", "%1470 6%", "%1480 6%", "%1544 9%", "%1589 8%", "%1600 6%", "%1610 6%", "%1600 6%",
"%1670 6%", "%1680 6%", "%1690 6%", "%1710 6%", "%1720 6%", "%1742 9%", "%1780 5%", "%1790 6%", "%1810 6%", "%1820 6%", "%1830 6%",
"%1870 6%", "%1910 6%", "%1940 6%", "%1950 6%", "%1953 0%", "%1980 6%", "%1990 6%", "%3000 6%"]

caisse_epargne = ["%1131 5%", "%1142 5%", "%1213 5%", "%1313 5%", "%1333 5%", "%1348 5%", "%1382 5%", "%1426 5%", "%1444 5%", "%1450 5%",
"%1513 5%", "%1627 5%", "%1670 5%", "%1751 5%", "%1802 5%", "%1831 5%", "%1871 5%", "%1982 5%", "%1621 0"]

bnp = ["%1149 8%", "%1172 9%", "%1307 8%", "%1308 8%", "%1540 8%", "%1566 8%", "%1593 8%", "%1607 8%", "%1793 9%", "%1802 0%", "%1802 9%",
"%3000 4%", "%3059 8%", "%4019 8%", "%4132 9%", "%4191 9%"]

credit_mutuel = ["%1027 8%", "%1162 8%", "%1180 8%", "%1542 9%", "%1545 9%", "%1548 9%", "%1551 9%", "%1554 9%", "%1558 9%", "%1562 9%",
"%1574 9%", "%1582 9%", "%1589 9%", "%1595 9%", "%1608 8%", "%1615 9%", "%1617 9%", "%4553 9%"]

cic = ["%1160 0%", "%1307 0%", "%1584 8%", "%1723 0%", "%3000 6%", "%3008 7%", "%4119 9%", "%3004 7%", "%1005 7%"]
banque_postale = ["%1617 8%", "%2004 1%"]
societe_generale = ["%1376 9%", "%1486 9%", "%1596 8%", "%1807 9%", "%1831 9%", "%1999 0%", "%3000 3%"]
boursorama = ["%4061 8%"]
qonto = ["%1695 8%", "%1659 8%"]
lydia = ["%1759 8%"]
revolut = ["%2823 3%"]
lcl = ["%3000 2%", "%1009 6%"]
monabanq = ["%1469 0%"]
bforbank =["%1621 8%"]
shine = ["%1741 8%"]

df_responsables = df_responsables.withColumn("RE_CSP1", coalesce(col("RE_CSP1"), col("RE_CSP2"), lit(99))) \
                                 .withColumnRenamed("RE_NOM1", "NOM") \
                                 .withColumnRenamed("RE_PRENOM1", "PRENOM") \
                                 .withColumnRenamed("RE_CSP1", "IDPROFESSION") \
                                 .withColumnRenamed("RE_MODE_REGLEMENT", "REGLEMENT") \
                                 .withColumnRenamed("RE_ENF_A_CHARGE", "ENFANTSACHARGE") \
                                 .withColumnRenamed("RE_TELPORTABLE1", "TELEPHONE") \
                                 .withColumnRenamed("RE_EMAILPERSO1", "EMAIL") \
                                 .withColumnRenamed("RE_IBAN", "NUMEROCOMPTE") \
                                 .withColumn("KEYRESPONSABLE", concat(col("SCHOOLYEAR"),
                                                               lit("-"),
                                                               col("IDRESPONSABLE"))) \
                                 .withColumn("CODEPOSTAL", when(col("RE_CODEPOSTAL") == "H4V1H2", None).otherwise(col("RE_CODEPOSTAL"))) \
                                 .join(df_foyers, on = "IDFOYER", how = "left")     

df_responsables = df_responsables.withColumn("BANQUE", when(reduce(or_,[col("NUMEROCOMPTE").like(p) for p in credit_mutuel]),"CREDIT MUTUEL") \
                                  .when(reduce(or_,[col("NUMEROCOMPTE").like(p) for p in banques_populaires]), "BANQUE POPULAIRE") \
                                  .when(reduce(or_,[col("NUMEROCOMPTE").like(p) for p in credit_agricole]), "CREDIT AGRICOLE") \
                                  .when(reduce(or_,[col("NUMEROCOMPTE").like(p) for p in caisse_epargne]), "CAISSE D'EPARGNE")\
                                  .when(reduce(or_,[col("NUMEROCOMPTE").like(p) for p in bnp]), "BNP") \
                                  .when(reduce(or_,[col("NUMEROCOMPTE").like(p) for p in cic]), "CIC") \
                                  .when(reduce(or_,[col("NUMEROCOMPTE").like(p) for p in banque_postale]), "BANQUE POSTALE") \
                                  .when(reduce(or_,[col("NUMEROCOMPTE").like(p) for p in societe_generale]), "SOCIETE GENERALE") \
                                  .when(reduce(or_,[col("NUMEROCOMPTE").like(p) for p in boursorama]), "BOURSORAMA") \
                                  .when(reduce(or_,[col("NUMEROCOMPTE").like(p) for p in qonto]), "QONTO") \
                                  .when(reduce(or_,[col("NUMEROCOMPTE").like(p) for p in lydia]), "LYDIA") \
                                  .when(reduce(or_,[col("NUMEROCOMPTE").like(p) for p in revolut]), "REVOLUT") \
                                  .when(reduce(or_,[col("NUMEROCOMPTE").like(p) for p in lcl]), "LCL") \
                                  .when(reduce(or_,[col("NUMEROCOMPTE").like(p) for p in monabanq]), "MONABANQ") \
                                  .when(reduce(or_,[col("NUMEROCOMPTE").like(p) for p in bforbank]), "BFORBANK") \
                                  .when(reduce(or_,[col("NUMEROCOMPTE").like(p) for p in shine]), "SHINE") \
                                  .otherwise("AUTRES"))

df_responsables = df_responsables.withColumn("TELEPHONE", regexp_replace(col("TELEPHONE"), r"[\s-]", "")) \
                                 .withColumn("TELEPHONE", regexp_replace(col("TELEPHONE"), r"^\+(590|596|594|33)", "0")) \
                                 .withColumn("TELEPHONE", regexp_replace(col("TELEPHONE"), r"\?", "")) 

df_parents = df_responsables.withColumn("FULLNAME", concat(col("NOM"), lit(" "), col("PRENOM"))) \
                            .select(col("IDRESPONSABLE").cast(IntegerType()), 
                                    "NOM", 
                                    "PRENOM",
                                    "FULLNAME")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ecoliers = df_ecoliers.withColumn("IDELEVE", when(col("IDELEVE")== 575, lit(668)).otherwise(col("IDELEVE")))

df_ecoliers = df_ecoliers.join(df_factures_eleves, on=["IDELEVE", "SCHOOLYEAR"], how="left") \
                         .join(broadcast(df_classes), df_ecoliers["EL_IDCLASSE"] == df_classes["IDCLASSE"], "left")\
                         .join(broadcast(df_regimes), df_ecoliers["EL_IDREGIME"] == df_regimes["IDREGIME"], "left") \
                         .withColumnRenamed("EL_NOM1", "NOM")\
                         .withColumnRenamed("EL_PRENOM1", "PRENOM")\
                         .withColumnRenamed("EL_SEXE", "SEXE")\
                         .withColumnRenamed("EL_DATE_DE_NAISSANCE", "DATENAISSANCE") \
                         .withColumnRenamed("EL_DATE_ENTREE", "DATEENTREE") \
                         .withColumnRenamed("EL_DATE_SORTIE", "DATESORTIE") \
                         .withColumnRenamed("EL_NATIONALITE1", "NATIONALITE") \
                         .withColumnRenamed("EL_IDENT_NAT", "IDENTITENATIONALE") \
                         .withColumn("DATEENTREE", parse_date("DATEENTREE")) \
                         .withColumn("DATESORTIE", parse_date("DATESORTIE")) \
                         .withColumn("DATENAISSANCE", parse_date("DATENAISSANCE")) \
                         .withColumn("IDREGIME", when(col("EL_IDREGIME").isNull(), 2).otherwise(col("EL_IDREGIME"))) \
                         .withColumn("REGIME", when(col("CLASSE") == "AE", "EXTERNE").otherwise(col("REGIME"))) \
                         .withColumn("AGE", when(col("DATENAISSANCE").isNotNull() & age_years.between(0,120), age_years).otherwise(lit(None))) \
                         .withColumn("KEYELEVE", concat(df_factures_eleves["SCHOOLYEAR"],
                                                   lit("-"),
                                                   col("IDELEVE")))

df_enfants = df_ecoliers.withColumn("FULLNAME", concat(col("NOM"), lit(" "), col("PRENOM"))) \
                        .select(col("IDELEVE").cast(IntegerType()), 
                                "NOM", 
                                "PRENOM", 
                                "SEXE",
                                "DATENAISSANCE",
                                "AGE",
                                "NATIONALITE",
                                "IDENTITENATIONALE",
                                "FULLNAME")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

start_date = "20220101"
end_date   = "20301231"

df_date_range = spark.createDataFrame([(start_date, end_date)], ["start_date", "end_date"])
df_dates = df_date_range.select(
    explode(sequence(to_date(col("start_date"), "yyyyMMdd"),
                     to_date(col("end_date"), "yyyyMMdd"),
                     expr("interval 1 day"))).alias("DATE"))

window_dates = Window.orderBy("DATE")

df_dates = df_dates.withColumn("CALENDARYEAR", year(col("DATE"))) \
                   .withColumn("IDDATE", row_number().over(window_dates)) \
                   .withColumn("CALENDARMONTH", month(col("DATE"))) \
                   .withColumn("CALENDARDAY", dayofmonth(col("DATE"))) \
                   .withColumn("MONTHNAME", date_format(col("DATE"), "MMMM")) \
                   .withColumn("DAYNAME", date_format(col("DATE"), "EEEE")) \
                   .withColumn("SCHOOLYEARSTART", when(month(col("DATE")) >= 8, year(col("DATE"))).otherwise(year(col("DATE")) -1)) \
                   .withColumn("SCHOOLYEAREND", col("SCHOOLYEARSTART") + 1) \
                   .withColumn("SCHOOLYEAR", concat(col("SCHOOLYEARSTART"),
                                             lit("-"),
                                             col("SCHOOLYEAREND"))) \
                   .withColumn("SCHOOLYEARMONTH", when(month(col("DATE")) >= 9, month(col("DATE")) - 8).otherwise(month(col("DATE")) + 4)) \
                   .withColumn("ISSCHOOLPERIOD", when((month(col("DATE")) >= 9) | (month(col("DATE")) <= 6), 1).otherwise(0)) \
                   .select("IDDATE","DATE", "CALENDARYEAR", "CALENDARMONTH", "CALENDARDAY", "MONTHNAME", "DAYNAME", "SCHOOLYEAR", "ISSCHOOLPERIOD")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

school_years_schema = StructType([
    StructField("SCHOOLYEAR", StringType(), True),
    StructField("SCHOOLYEARLIBELLE", StringType(), True)
])

school_years_data = [
    ("2021-2022", "SCHOOL YEAR 2021-2022"),
    ("2022-2023", "SCHOOL YEAR 2022-2023"),
    ("2023-2024", "SCHOOL YEAR 2023-2024"),
    ("2024-2025", "SCHOOL YEAR 2024-2025"),
    ("2025-2026", "SCHOOL YEAR 2025-2026"),
    ("2026-2027", "SCHOOL YEAR 2026-2027"),
    ("2027-2028", "SCHOOL YEAR 2027-2028"),
    ("2028-2029", "SCHOOL YEAR 2028-2029"),
    ("2029-2030", "SCHOOL YEAR 2029-2030")
    ]

df_school_years = spark.createDataFrame(school_years_data, schema = school_years_schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_factures_familles = df_factures_familles.withColumnRenamed("HF_APAYER_FACTURE", "TOTALFAMILLE") \
                                           .withColumn("DATEFACTURE", to_date(col("HF_DATE_FACTURE"), "yyyyMMdd")) \
                                           .withColumn("KEYRESPONSABLE", concat(col("SCHOOLYEAR"),
                                                                    lit("-"),
                                                                    col("IDRESPONSABLE"))) \
                                           .withColumn("KEYVALIDATION", concat(col("SCHOOLYEAR"),
                                                                        lit("-"),
                                                                        col("IDVALIDATION"))) \
                                           .join(df_responsables, on = ["KEYRESPONSABLE", "IDRESPONSABLE"], how = "left")                                 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_factures_eleves = df_factures_eleves.withColumn("HE_IDCLASSE", when((col("HE_IDCLASSE")== 11) & (col("SCHOOLYEAR")== "2024-2025"), 25) \
                                       .when((col("HE_IDCLASSE")== 9) & (col("SCHOOLYEAR")== "2024-2025"), 24) \
                                       .when((col("HE_IDCLASSE")== 23) & (col("SCHOOLYEAR") == "2024-2025"), 26) \
                                       .otherwise(col("HE_IDCLASSE")))

df_factures_eleves = df_factures_eleves.withColumn("IDELEVE", when(col("IDELEVE")== 575, lit(668)).otherwise(col("IDELEVE")))

df_factures_eleves = df_factures_eleves.withColumnRenamed("HE_IDREGIME", "IDREGIME") \
                                       .withColumnRenamed("HE_IDCLASSE", "IDCLASSE") \
                                       .withColumnRenamed("HE_APAYER_ELEVE", "TOTALELEVE")\
                                       .withColumn("KEYVALIDATION", concat(col("SCHOOLYEAR"),
                                                                    lit("-"),
                                                                    col("IDVALIDATION"))
                                                                    .cast("string")) \
                                       .withColumn("KEYRESPONSABLE", concat(col("SCHOOLYEAR"),
                                                                    lit("-"),
                                                                    col("IDRESPONSABLE"))
                                                                    .cast("string")) \
                                       .withColumn("KEYELEVE", concat(col("SCHOOLYEAR"),
                                                               lit("-"),
                                                               col("IDELEVE"))) \
                                       .withColumn("KEYCLASSE", concat(col("SCHOOLYEAR"),
                                                               lit("-"),
                                                               col("IDCLASSE"))) \
                                       .join(df_factures_familles, on = ["KEYVALIDATION", "IDVALIDATION", "KEYRESPONSABLE", "IDRESPONSABLE", "SCHOOLYEAR"], how = "left") \
                                       .filter(col("IDELEVE") != 0) \
                                       .withColumn("IDREGIME", when(col("IDREGIME") == 0, 2).otherwise(col("IDREGIME")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

voyage = ["CM2_TRIP", "CM2TRIP", "VOYAGE_LING_FLL", "VOYAGE_LING_DOMINICA", "VOYAGE_LING_FTL", "CM1VL", "VLMFL", "VLCM2", "VL_ATL", "VOYAGES"]
cantine = ["REPAS_THANKSGIVING"]
frais = ["FRAIS_INS", "FRAIS_REINSC", "FRAIS_REINSCR", "FRAISRETARD", "PENALITE","LMS", "ACCES_ED", "FRAISREJET" ]
uniforme = ["JUPES", "UNIFORME", "POLO", "POLOS", "SHORT", "T_SHIRT", "SORCT", "JUPE"]
etude = ["ACADEMIC_WEDNESDAY"]
psg =["PSG_COMPLET", "PSG_DEMI_JOURNEE", "EXT_PSG_COMPLET", "EXT_PSG_DEMI"]
sortie = ["SORTIES", "KAYAK", "CINETHEATRE_MILETOIL"]
cambridge = ["CAMBDRIDGEEXAM", "CAMBRIDGEEXAM"]
wanted_services =["FRAISRETARD", "VOYAGES"]
services_from_lines = df_factures_services.select(col("HL_CODE_LIGNE").alias("SERVICE"))
services_from_levels = df_factures_niveaux.filter(col("CG_POSTE_ANA").isin(wanted_services)) \
                                          .select(col("CG_POSTE_ANA").alias("SERVICE"))

all_services = services_from_lines.union(services_from_levels).distinct()

df_factures_services = df_factures_services.drop("SERVICE") \
                                           .join(all_services, df_factures_services["HL_CODE_LIGNE"] == all_services["SERVICE"], "left")


df_factures_services = df_factures_services.withColumn("SERVICE", regexp_replace(col("SERVICE"), "BABY_LISE|EXT_BABYLISE", "BABY LISE")) \
                                           .withColumn("SERVICE", regexp_replace(col("SERVICE"), "EXT_OUTDOOR|OUTDOOR", "OUTDOOR")) \
                                           .withColumn("SERVICE", regexp_replace(col("SERVICE"), "FOURNITURES", "FOURNITURE")) 
        
df_factures_services = df_factures_services.withColumn("SERVICE",
                                            when(col("SERVICE").isin(voyage), "VOYAGE") 
                                            .when(col("SERVICE").isin(cantine), "CANTINE") 
                                            .when(col("SERVICE").isin(uniforme), "UNIFORME") 
                                            .when(col("SERVICE").isin(psg), "PSG") 
                                            .when(col("SERVICE").isin(etude), "ETUDE")
                                            .when(col("SERVICE").isin(frais), "FRAIS") 
                                            .when(col("SERVICE").isin(sortie), "SORTIE")
                                            .when(col("SERVICE").isin(cambridge), "CAMBRIDGE")
                                            .otherwise(col("SERVICE"))) 

df_factures_services = df_factures_services.withColumnRenamed("HL_QUANTITE", "QUANTITE") \
                                           .withColumnRenamed("HL_PRIX", "PRIX") \
                                           .withColumnRenamed("HL_REMISE_MT_AUTO", "REMISE") \
                                           .withColumnRenamed("HL_APAYER_LIGNE", "TOTALSERVICE") \
                                           .withColumn("KEYRESPONSABLE", concat(col("SCHOOLYEAR"),
                                                                    lit("-"),
                                                                    col("IDRESPONSABLE"))
                                                                    .cast("string")) \
                                           .withColumn("KEYVALIDATION", concat(col("SCHOOLYEAR"),
                                                                        lit("-"),
                                                                        col("IDVALIDATION"))
                                                                        .cast("string")) \
                                           .withColumn("KEYELEVE", concat(col("SCHOOLYEAR"),
                                                                   lit("-"),
                                                                   col("IDELEVE"))) \
                                          .join(df_services, on = "SERVICE", how = "left") \
                                          .join(df_factures_eleves, on = ["KEYELEVE", "IDELEVE", "KEYRESPONSABLE", "IDRESPONSABLE", "KEYVALIDATION", "IDVALIDATION", "SCHOOLYEAR"], how ="left") 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

unwanted_niveaux = ["FRAISRETARD", "VOYAGES"]

df_factures_niveaux = df_factures_niveaux.filter(~col("CG_POSTE_ANA").isin(unwanted_niveaux)) \
                                         .withColumn("TOTALNIVEAU", (col("CG_CREDIT") - col("CG_DEBIT")))\
                                         .withColumn("NIVEAU", regexp_replace(col("CG_POSTE_ANA"),"TPS", "MATERNELLE"))\
                                         .withColumn("DATEFACTURE", to_date(col("CG_DATE_FACTURE"), "yyyyMMdd")) \
                                         .withColumn("KEYRESPONSABLE", concat(col("SCHOOLYEAR"),
                                                                    lit("-"),
                                                                    col("IDRESPONSABLE"))
                                                                    .cast("string")) \
                                         .withColumn("KEYVALIDATION", concat(col("SCHOOLYEAR"),
                                                                    lit("-"),
                                                                    col("IDVALIDATION"))
                                                                    .cast("string")) \
                                         .join(broadcast(df_niveaux), on = "NIVEAU", how="left") 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_factures_validations = df_factures_validations.withColumn("TYPEFACTURE", regexp_replace(col("VA_TYPE_FACTURE"), "Toutes", "Calculées")) \
                                                 .withColumnRenamed("VA_NB_FACTURES", "NOMBREFACTURE") \
                                                 .withColumnRenamed("VA_DATE_HEURE", "DATEVALIDATION") \
                                                 .withColumn("DATEVALIDATION", regexp_replace(col("DATEVALIDATION"), "Le", "")) \
                                                 .withColumn("DATEVALIDATION", regexp_replace(col("DATEVALIDATION"), "à", "")) \
                                                 .withColumn("DATEVALIDATION", trim(col("DATEVALIDATION"))) \
                                                 .withColumn("DATEVALIDATION", split(col("DATEVALIDATION"), " ").getItem(0)) \
                                                 .withColumn("DATEVALIDATION", to_date(col("DATEVALIDATION"), "dd/MM/yyyy")) \
                                                 .withColumn("KEYVALIDATION", concat(col("SCHOOLYEAR"),
                                                                    lit("-"),
                                                                    col("IDVALIDATION"))
                                                                    .cast("string")) \
                                                 .join(df_factures_familles, on=["KEYVALIDATION", "IDVALIDATION"], how = "left")                                               

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_factures_familles = df_factures_familles.select("KEYRESPONSABLE",
                                                   col("IDRESPONSABLE").cast(IntegerType()),
                                                   "KEYVALIDATION",
                                                   col("IDVALIDATION").cast(IntegerType()),
                                                   col("IDFOYER").cast(IntegerType()), 
                                                   col("IDPROFESSION").cast(IntegerType()),
                                                   col("TOTALFAMILLE").cast(FloatType()), 
                                                   "DATEFACTURE")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_responsables = df_responsables.select("KEYRESPONSABLE", 
                                         col("IDRESPONSABLE").cast(IntegerType()),
                                         col("ENFANTSACHARGE").cast(DoubleType()),
                                         "REGLEMENT",
                                         col("TELEPHONE").cast(IntegerType()),
                                         "EMAIL",
                                         "NUMEROCOMPTE",
                                         "BANQUE")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_factures_services = df_factures_services.select("KEYELEVE",
                                                   col("IDELEVE").cast(IntegerType()),
                                                   "KEYRESPONSABLE",
                                                   col("IDRESPONSABLE").cast(IntegerType()), 
                                                   "KEYVALIDATION",
                                                   col("IDVALIDATION").cast(IntegerType()),  
                                                   col("IDSERVICE").cast(IntegerType()), 
                                                   col("QUANTITE").cast(FloatType()),
                                                   col("PRIX").cast(FloatType()), 
                                                   col("REMISE").cast(FloatType()), 
                                                   col("TOTALSERVICE").cast(FloatType()), 
                                                   "DATEFACTURE") 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_factures_eleves = df_factures_eleves.select("KEYELEVE",
                                               col("IDELEVE").cast(IntegerType()),                                               
                                               "KEYRESPONSABLE",
                                               col("IDRESPONSABLE").cast(IntegerType()),  
                                               "KEYVALIDATION",
                                               col("IDVALIDATION").cast(IntegerType()),
                                               "KEYCLASSE",
                                               col("IDCLASSE").cast(IntegerType()),
                                               col("IDREGIME").cast(IntegerType()),  
                                               col("TOTALELEVE").cast(FloatType()), 
                                               "DATEFACTURE")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_eleves = df_ecoliers.select("KEYELEVE", 
                               col("IDELEVE").cast(IntegerType()), 
                               col("IDRESPONSABLE").cast(IntegerType()), 
                               "DATEENTREE", 
                               "DATESORTIE") \
                       .filter(col("IDRESPONSABLE").isNotNull() & (trim(col("IDRESPONSABLE")) != ""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_factures_niveaux = df_factures_niveaux.select(col("IDNIVEAU").cast(IntegerType()), 
                                                 "KEYVALIDATION",
                                                 col("IDVALIDATION").cast(IntegerType()),  
                                                 "KEYRESPONSABLE",
                                                 col("IDRESPONSABLE").cast(IntegerType()), 
                                                 col("TOTALNIVEAU").cast(FloatType()), 
                                                 "DATEFACTURE")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_factures_validations = df_factures_validations.select("KEYVALIDATION", 
                                                         col("IDVALIDATION").cast(IntegerType()), 
                                                         "TYPEFACTURE", 
                                                         col("NOMBREFACTURE").cast(IntegerType()), 
                                                         "DATEVALIDATION")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_factures_eleves = df_factures_eleves.filter(col("DATEFACTURE").isNotNull())
df_factures_familles = df_factures_familles.filter(col("DATEFACTURE").isNotNull())
df_factures_services = df_factures_services.filter(col("DATEFACTURE").isNotNull())
df_factures_niveaux = df_factures_niveaux.filter(col("DATEFACTURE").isNotNull())
df_factures_validations = df_factures_validations.filter(col("DATEVALIDATION").isNotNull())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_factures_eleves = df_factures_eleves.dropDuplicates()
df_factures_familles = df_factures_familles.dropDuplicates()
df_factures_niveaux = df_factures_niveaux.dropDuplicates()
df_factures_services = df_factures_services.dropDuplicates()
df_factures_validations = df_factures_validations .dropDuplicates()
df_dates = df_dates.dropDuplicates(subset=["IDDATE"])
df_enfants = df_enfants.dropDuplicates(subset=["IDELEVE"])
df_eleves = df_eleves.dropDuplicates(subset=["KEYELEVE"])
df_regimes = df_regimes.dropDuplicates(subset=["IDREGIME"])
df_foyers = df_foyers.dropDuplicates(subset=["IDFOYER"])
df_personnels = df_personnels.dropDuplicates(subset=["IDPERSONNEL"])
df_professeurs = df_professeurs.dropDuplicates(subset=["IDPROFESSEUR"])
df_staff = df_staff.dropDuplicates(subset=["KEYPERSONNEL"])
df_pays = df_pays.dropDuplicates(subset=["IDPAYS"])
df_professions = df_professions.dropDuplicates(subset=["IDPROFESSION"])
df_etablissements = df_etablissements.dropDuplicates(subset=["IDETABLISSEMENT"]) 
df_classes = df_classes.dropDuplicates(subset=["IDCLASSE"])
df_classes_targets = df_classes_targets.dropDuplicates(subset=["KEYCLASSE"])
df_niveaux = df_niveaux.dropDuplicates(subset=["IDNIVEAU"]) 
df_services = df_services.dropDuplicates(subset=["IDSERVICE"])
df_villes = df_villes.dropDuplicates(subset=["IDVILLE"])
df_parents = df_parents.dropDuplicates(subset=["IDRESPONSABLE"])
df_responsables = df_responsables.dropDuplicates(subset=["KEYRESPONSABLE"])
df_school_years = df_school_years.dropDuplicates(subset=["SCHOOLYEAR"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

overwrite_tables = {
    "dim_classes": df_classes,
    "dim_dates": df_dates,
    "dim_foyers": df_foyers,
    "dim_villes": df_villes,
    "dim_services": df_services,
    "dim_etablissements": df_etablissements,
    "dim_niveaux": df_niveaux,
    "dim_professions": df_professions,
    "dim_personnels": df_personnels,
    "dim_professeurs": df_professeurs,
    "dim_staff": df_staff,
    "dim_pays": df_pays,
    "dim_regimes": df_regimes,
    "dim_enfants": df_enfants,
    "dim_eleves": df_eleves,
    "dim_parents": df_parents,
    "dim_responsables": df_responsables,
    "dim_classes_targets": df_classes_targets,
    "dim_school_years": df_school_years
}

append_tables = {
    "fact_factures_eleves": df_factures_eleves,
    "fact_factures_niveaux": df_factures_niveaux,
    "fact_factures_familles": df_factures_familles,
    "fact_factures_services": df_factures_services,
    "fact_factures_validations": df_factures_validations
}

fact_key_cols = {
    "fact_factures_eleves": ["KEYELEVE", "KEYRESPONSABLE", "KEYVALIDATION"],
    "fact_factures_niveaux": ["IDNIVEAU", "KEYRESPONSABLE", "KEYVALIDATION"],
    "fact_factures_familles": ["KEYRESPONSABLE", "KEYVALIDATION"],
    "fact_factures_services": ["KEYELEVE", "KEYRESPONSABLE", "KEYVALIDATION"],
    "fact_factures_validations": ["KEYVALIDATION"]
}


for table_name, overwrite_df in overwrite_tables.items():
    try:
        overwrite_df.write.mode("overwrite").saveAsTable(f"{table_name}")
        print(f"Table {table_name} overwritten successfully.")
    except Exception as e:
        print(f"Error overwriting table {table_name}: {e}")

def make_merge_condition(keys):
    return " AND ".join([f"t.{col} = s.{col}" for col in keys])

for table_name, append_df in append_tables.items():
    keys = fact_key_cols.get(table_name)
    if not keys:
        raise ValueError(f"No business key defined for table {table_name}")

    merge_condition = make_merge_condition(keys)

    try:
        target = DeltaTable.forName(spark, table_name)
        (target.alias("t").merge(append_df.alias("s"), merge_condition).whenNotMatchedInsertAll().execute())
        print(f"Upsert completed for '{table_name}' using key columns {keys}")
    except Exception as e:
        if "is not a Delta table" in e.desc:
            append_df.write.mode("overwrite").saveAsTable(f"{table_name}")
            print(f"Created new Delta table {table_name}")
        else:
            print(f"Error upserting '{table_name}': {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

rows_processed = {
    "dim_classes": df_classes.count(),
    "dim_classes_targets": df_classes_targets.count(),
    "dim_dates": df_dates.count(),
    "dim_foyers": df_foyers.count(),
    "dim_villes": df_villes.count(),
    "dim_services": df_services.count(),
    "dim_etablissements": df_etablissements.count(),
    "dim_niveaux": df_niveaux.count(),
    "dim_professions": df_professions.count(),
    "dim_personnels": df_personnels.count(),
    "dim_professeurs": df_professeurs.count(),
    "dim_staff": df_staff.count(),
    "dim_pays": df_pays.count(),
    "dim_regimes": df_regimes.count(),
    "dim_enfants": df_enfants.count(),
    "dim_eleves": df_eleves.count(),
    "dim_parents": df_parents.count(),
    "dim_responsables": df_responsables.count(),
    "dim_school_years": df_school_years.count(),
    "fact_factures_eleves": df_factures_eleves.count(),
    "fact_factures_niveaux": df_factures_niveaux.count(),
    "fact_factures_familles": df_factures_familles.count(),
    "fact_factures_services": df_factures_services.count(),
    "fact_factures_validations": df_factures_validations.count()
}

total_rows_processed = __builtins__.sum(rows_processed.values())
print(f"Total rows processed: {total_rows_processed}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

run_id = str(uuid.uuid4())
run_ts = datetime.now(ZoneInfo("America/New_York"))

result = {
    "status": "succeeded",
    "run_id": run_id,
    "run_ts": run_ts.isoformat(),
    "rows_processed": rows_processed,
    "total_rows_processed": total_rows_processed
}

mssparkutils.notebook.exit(json.dumps(result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }
