# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5b11fd41-c6a9-48d3-968c-c2b8633746e8",
# META       "default_lakehouse_name": "LH_SILVER",
# META       "default_lakehouse_workspace_id": "ca671cc1-0874-4ef0-aac1-5c341115234f",
# META       "known_lakehouses": [
# META         {
# META           "id": "5b11fd41-c6a9-48d3-968c-c2b8633746e8"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

export_base = "Files/snowflake_export"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dates = spark.read.table("dim_dates")

df_dates.coalesce(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .save(f"{export_base}/dates")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_etablissements = spark.read.table("dim_etablissements")

df_etablissements.coalesce(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .save(f"{export_base}/etablissements")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_niveaux = spark.read.table("dim_niveaux")

df_niveaux.coalesce(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .save(f"{export_base}/niveaux")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_classes = spark.read.table("dim_classes")

df_classes.coalesce(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .save(f"{export_base}/classes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_professiontypes = spark.read.table("dim_professions")

df_professiontypes.coalesce(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .save(f"{export_base}/professiontypes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_regimes = spark.read.table("dim_regimes")

df_regimes.coalesce(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .save(f"{export_base}/regimes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_services = spark.read.table("dim_services")

df_services.coalesce(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .save(f"{export_base}/services")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_personnels = spark.read.table("dim_personnels")

df_personnels.coalesce(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .save(f"{export_base}/personnels")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_payroll = spark.read.table("fact_payroll_current")

df_payroll.coalesce(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .save(f"{export_base}/payroll")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_factureseleves = spark.read.table("fact_factures_eleves")

df_factureseleves.coalesce(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .save(f"{export_base}/factureseleves")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_facturesservices = spark.read.table("fact_factures_services")

df_facturesservices.coalesce(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .save(f"{export_base}/facturesservices")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("DATES:", df_dates.count())
print("ETABLISSEMENTS:", df_etablissements.count())
print("NIVEAUX:", df_niveaux.count())
print("CLASSES:", df_classes.count())
print("PROFESSIONTYPES:", df_professiontypes.count())
print("REGIMES:", df_regimes.count())
print("SERVICES:", df_services.count())
print("PERSONNELS:", df_personnels.count())
print("PAYROLL:", df_payroll.count())
print("FACTURESELEVES:", df_factureseleves.count())
print("FACTURESSERVICES:", df_facturesservices.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
