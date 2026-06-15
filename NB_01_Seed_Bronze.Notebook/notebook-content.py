# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Fabric Notebook: NB_01_Seed_Bronze
# Purpose: Create Bronze Delta tables from inline sample data (no external source needed)
# Layer: Bronze — raw, source-oriented ingestion
# ──────────────────────────────────────────────────────────────────
# ⚠️ Set Bronze_LH as the DEFAULT lakehouse for this notebook (Lakehouses pane →
#    pin Bronze_LH as default). Tables are written with single-part names so they
#    land in the default lakehouse — this avoids the schema-enabled lakehouse parsing
#    a two-part name like "Bronze_LH.production_raw" as schema.table ([SCHEMA_NOT_FOUND]).
# ──────────────────────────────────────────────────────────────────────────

# Cell 1 — Imports & Spark session
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, DateType, IntegerType
)
from pyspark.sql.functions import current_timestamp, lit
from datetime import date

spark = SparkSession.builder.getOrCreate()
print(f"Spark version: {spark.version}")

# Cell 2 — Production Raw Data (simulates PIMS source)
production_schema = StructType([
    StructField("well_id",   StringType(), True),
    StructField("date",      DateType(),   True),
    StructField("field",     StringType(), True),
    StructField("oil_bbl",   DoubleType(), True),
    StructField("gas_mcf",   DoubleType(), True),
    StructField("water_bbl", DoubleType(), True),
    StructField("status",    StringType(), True),
])

production_rows = [
    ("W-001", date(2024,1,1),  "Oseberg",  1200.0, 4500.0, 320.0, "Active"),
    ("W-001", date(2024,1,2),  "Oseberg",  1185.0, 4420.0, 318.0, "Active"),
    ("W-001", date(2024,1,3),  "Oseberg",  1210.0, 4510.0, 321.0, "Active"),
    ("W-001", date(2024,1,4),  "Oseberg",  1198.0, 4490.0, 319.0, "Active"),
    ("W-001", date(2024,1,5),  "Oseberg",  1202.0, 4505.0, 320.0, "Active"),
    ("W-002", date(2024,1,1),  "Troll",     980.0, 6200.0, 150.0, "Active"),
    ("W-002", date(2024,1,2),  "Troll",     975.0, 6185.0, 148.0, "Active"),
    ("W-002", date(2024,1,3),  "Troll",     982.0, 6210.0, 151.0, "Active"),
    ("W-002", date(2024,1,4),  "Troll",     970.0, 6180.0, 147.0, "Active"),
    ("W-002", date(2024,1,5),  "Troll",     978.0, 6195.0, 149.0, "Active"),
    ("W-003", date(2024,1,1),  "Gullfaks", 2100.0, 3800.0, 510.0, "Active"),
    ("W-003", date(2024,1,2),  "Gullfaks", 2085.0, 3790.0, 508.0, "Active"),
    ("W-003", date(2024,1,3),  "Gullfaks", 2110.0, 3810.0, 512.0, "Active"),
    ("W-003", date(2024,1,4),  "Gullfaks", 2095.0, 3800.0, 510.0, "Active"),
    ("W-003", date(2024,1,5),  "Gullfaks", 2105.0, 3805.0, 511.0, "Active"),
    ("W-004", date(2024,1,1),  "Snorre",    450.0, 1200.0,  90.0, "Maintenance"),
    ("W-004", date(2024,1,2),  "Snorre",      0.0,    0.0,   0.0, "Shut-in"),
    ("W-004", date(2024,1,3),  "Snorre",      0.0,    0.0,   0.0, "Shut-in"),
    ("W-004", date(2024,1,4),  "Snorre",    200.0,  600.0,  45.0, "Active"),
    ("W-004", date(2024,1,5),  "Snorre",    430.0, 1180.0,  88.0, "Active"),
    ("W-005", date(2024,1,1),  "Ekofisk",  1650.0, 5100.0, 275.0, "Active"),
    ("W-005", date(2024,1,2),  "Ekofisk",  1640.0, 5090.0, 272.0, "Active"),
    ("W-005", date(2024,1,3),  "Ekofisk",  1655.0, 5110.0, 276.0, "Active"),
    ("W-005", date(2024,1,4),  "Ekofisk",  1645.0, 5095.0, 273.0, "Active"),
    ("W-005", date(2024,1,5),  "Ekofisk",  1648.0, 5098.0, 274.0, "Active"),
    ("W-006", date(2024,1,1),  "Oseberg",   890.0, 3200.0, 180.0, "Active"),
    ("W-006", date(2024,1,2),  "Oseberg",   885.0, 3195.0, 179.0, "Active"),
    ("W-006", date(2024,1,3),  "Oseberg",   892.0, 3205.0, 181.0, "Active"),
    ("W-006", date(2024,1,4),  "Oseberg",   888.0, 3198.0, 180.0, "Active"),
    ("W-006", date(2024,1,5),  "Oseberg",   891.0, 3202.0, 180.0, "Active"),
]

df_prod = spark.createDataFrame(production_rows, production_schema)
df_prod = df_prod.withColumn("ingested_at", current_timestamp()) \
                 .withColumn("source_system", lit("PIMS"))

df_prod.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
       .saveAsTable("Bronze_LH.dbo.production_raw")
print(f"✅ Bronze_LH.production_raw: {df_prod.count()} rows written")
display(df_prod)

# Cell 3 — Cost Raw Data (simulates Alpha source)
cost_schema = StructType([
    StructField("cost_id",    StringType(), True),
    StructField("date",       DateType(),   True),
    StructField("field",      StringType(), True),
    StructField("cost_type",  StringType(), True),
    StructField("amount_usd", DoubleType(), True),
    StructField("department", StringType(), True),
    StructField("project",    StringType(), True),
    StructField("vendor",     StringType(), True),
])

cost_rows = [
    ("C-001", date(2024,1,1), "Oseberg",  "OPEX",   45000.0, "Operations",  "PAB-2024",     "Schlumberger"),
    ("C-002", date(2024,1,1), "Troll",    "OPEX",   62000.0, "Operations",  "PAB-2024",     "Halliburton"),
    ("C-003", date(2024,1,1), "Gullfaks", "CAPEX", 250000.0, "Engineering", "GF-Expansion", "Baker Hughes"),
    ("C-004", date(2024,1,1), "Snorre",   "OPEX",   18000.0, "Maintenance", "SN-Maint-Q1",  "Internal"),
    ("C-005", date(2024,1,1), "Ekofisk",  "OPEX",   71000.0, "Operations",  "PAB-2024",     "Weatherford"),
    ("C-006", date(2024,1,2), "Oseberg",  "OPEX",   44500.0, "Operations",  "PAB-2024",     "Schlumberger"),
    ("C-007", date(2024,1,2), "Troll",    "OPEX",   61500.0, "Operations",  "PAB-2024",     "Halliburton"),
    ("C-008", date(2024,1,2), "Gullfaks", "OPEX",   38000.0, "Operations",  "PAB-2024",     "Internal"),
    ("C-009", date(2024,1,2), "Snorre",   "CAPEX",  95000.0, "Engineering", "SN-Upgrade",   "Baker Hughes"),
    ("C-010", date(2024,1,2), "Ekofisk",  "OPEX",   70500.0, "Operations",  "PAB-2024",     "Weatherford"),
    ("C-011", date(2024,1,3), "Oseberg",  "OPEX",   46000.0, "Operations",  "PAB-2024",     "Schlumberger"),
    ("C-012", date(2024,1,3), "Troll",    "OPEX",   63000.0, "Operations",  "PAB-2024",     "Halliburton"),
    ("C-013", date(2024,1,3), "Gullfaks", "CAPEX", 180000.0, "Engineering", "GF-Expansion", "Baker Hughes"),
    ("C-014", date(2024,1,3), "Snorre",   "OPEX",   22000.0, "Maintenance", "SN-Maint-Q1",  "Internal"),
    ("C-015", date(2024,1,3), "Ekofisk",  "OPEX",   69000.0, "Operations",  "PAB-2024",     "Weatherford"),
    ("C-016", date(2024,1,4), "Oseberg",  "OPEX",   44000.0, "Operations",  "PAB-2024",     "Schlumberger"),
    ("C-017", date(2024,1,4), "Troll",    "OPEX",   60500.0, "Operations",  "PAB-2024",     "Halliburton"),
    ("C-018", date(2024,1,4), "Gullfaks", "OPEX",   39000.0, "Operations",  "PAB-2024",     "Internal"),
    ("C-019", date(2024,1,4), "Snorre",   "OPEX",   19500.0, "Maintenance", "SN-Maint-Q1",  "Internal"),
    ("C-020", date(2024,1,4), "Ekofisk",  "OPEX",   72000.0, "Operations",  "PAB-2024",     "Weatherford"),
]

df_cost = spark.createDataFrame(cost_rows, cost_schema)
df_cost = df_cost.withColumn("ingested_at", current_timestamp()) \
                 .withColumn("source_system", lit("Alpha"))

df_cost.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
       .saveAsTable("Bronze_LH.dbo.cost_raw")
print(f"✅ Bronze_LH.cost_raw: {df_cost.count()} rows written")

# Cell 4 — Schedule Raw Data (simulates SharePoint source)
schedule_schema = StructType([
    StructField("schedule_id",  StringType(), True),
    StructField("start_date",   DateType(),   True),
    StructField("end_date",     DateType(),   True),
    StructField("activity",     StringType(), True),
    StructField("field",        StringType(), True),
    StructField("priority",     StringType(), True),
    StructField("status",       StringType(), True),
    StructField("assigned_to",  StringType(), True),
])

schedule_rows = [
    ("S-001", date(2024,1,5),  date(2024,1,10), "Well Inspection",          "Oseberg",  "High",     "Planned",     "Team Alpha"),
    ("S-002", date(2024,1,8),  date(2024,1,12), "Maintenance Shutdown",     "Snorre",   "Critical", "In-Progress", "Team Beta"),
    ("S-003", date(2024,1,15), date(2024,1,20), "Subsea Survey",            "Troll",    "Medium",   "Planned",     "External Vendor"),
    ("S-004", date(2024,1,20), date(2024,1,25), "Chemical Treatment",       "Gullfaks", "Low",      "Planned",     "Team Alpha"),
    ("S-005", date(2024,1,22), date(2024,1,28), "Production Test",          "Ekofisk",  "High",     "Planned",     "Team Gamma"),
    ("S-006", date(2024,2,1),  date(2024,2,5),  "Safety Audit",             "Oseberg",  "Critical", "Planned",     "HSE Team"),
    ("S-007", date(2024,2,10), date(2024,2,14), "Equipment Upgrade",        "Troll",    "High",     "Planned",     "Team Beta"),
    ("S-008", date(2024,2,15), date(2024,2,18), "Routine Inspection",       "Gullfaks", "Low",      "Planned",     "Team Alpha"),
    ("S-009", date(2024,2,20), date(2024,2,25), "Well Stimulation",         "Ekofisk",  "High",     "Planned",     "External Vendor"),
    ("S-010", date(2024,2,22), date(2024,2,26), "Pipeline Integrity Check", "Snorre",   "Critical", "Planned",     "Team Gamma"),
]

df_sched = spark.createDataFrame(schedule_rows, schedule_schema)
df_sched = df_sched.withColumn("ingested_at", current_timestamp()) \
                   .withColumn("source_system", lit("SharePoint"))

df_sched.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable("Bronze_LH.dbo.schedule_raw")
print(f"✅ Bronze_LH.schedule_raw: {df_sched.count()} rows written")

print("\n" + "="*60)
print("🏆 Bronze seeding COMPLETE")
print("Tables created: production_raw, cost_raw, schedule_raw")
print("Next: Run NB_02_Transform_Silver")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
