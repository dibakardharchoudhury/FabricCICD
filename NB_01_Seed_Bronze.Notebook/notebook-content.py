# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "cee2ea93-ccb4-4bb3-8338-4a91840b9509",
# META       "default_lakehouse_name": "Bronze_LH",
# META       "default_lakehouse_workspace_id": "292e18c3-b95e-42d1-bb02-9a2064fee5b8",
# META       "known_lakehouses": [
# META         {
# META           "id": "cee2ea93-ccb4-4bb3-8338-4a91840b9509"
# META         }
# META       ]
# META     }
# META   }
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
from datetime import date, timedelta
import random

spark = SparkSession.builder.getOrCreate()
print(f"Spark version: {spark.version}")

# Shared seeding window for ALL three Bronze tables. Data is generated deterministically
# (fixed random seeds below) so every run reproduces byte-identical Bronze tables — important
# for a repeatable CI/CD demo. Sized so EACH Bronze table lands at 50k+ rows.
_SEED_START = date(2024, 1, 1)
_SEED_DAYS  = 740                 # ~2 years of daily history

# field -> (well_count, baseline oil_bbl, gas_mcf, water_bbl). Shared by the cost + schedule cells.
# Well IDs are generated programmatically below, so well count per field is easy to scale.
_field_profiles = {
    "Oseberg":        (9, 1150.0, 4200.0, 300.0),
    "Troll":          (8,  980.0, 6200.0, 150.0),
    "Gullfaks":       (9, 2050.0, 3800.0, 510.0),
    "Snorre":         (8,  620.0, 1600.0,  95.0),
    "Ekofisk":        (9, 1640.0, 5100.0, 275.0),
    "Johan Sverdrup": (9, 2800.0, 2200.0, 180.0),
    "Grane":          (8,  760.0,  900.0, 410.0),
    "Heidrun":        (8, 1320.0, 3400.0, 230.0),
}

# field -> list of generated well IDs (e.g. "OSE-001"), unique across all fields. Total = 68 wells.
_field_wells = {
    _f: [f"{_f[:3].upper()}-{_i:03d}" for _i in range(1, _n + 1)]
    for _f, (_n, _o, _g, _w) in _field_profiles.items()
}

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

# Generate one row per well per day across the seeding window. ~2% of well-days are Shut-in
# (zero production, later filtered out in Silver) and ~4% are reduced-rate Maintenance; the rest
# are Active with +/-8% daily noise around each field's baseline.
random.seed(42)
production_rows = []
for _d in range(_SEED_DAYS):
    _day = _SEED_START + timedelta(days=_d)
    for _field, (_nwells, _oil, _gas, _water) in _field_profiles.items():
        for _w in _field_wells[_field]:
            _roll = random.random()
            if _roll < 0.02:                                  # shut-in: no production
                production_rows.append((_w, _day, _field, 0.0, 0.0, 0.0, "Shut-in"))
            elif _roll < 0.06:                                # maintenance: reduced rate
                _f = random.uniform(0.30, 0.60)
                production_rows.append((_w, _day, _field,
                    round(_oil * _f, 1), round(_gas * _f, 1), round(_water * _f, 1), "Maintenance"))
            else:                                             # normal active day
                production_rows.append((_w, _day, _field,
                    round(_oil * random.uniform(0.92, 1.08), 1),
                    round(_gas * random.uniform(0.92, 1.08), 1),
                    round(_water * random.uniform(0.92, 1.08), 1), "Active"))

df_prod = spark.createDataFrame(production_rows, production_schema)
df_prod = df_prod.withColumn("ingested_at", current_timestamp()) \
                 .withColumn("source_system", lit("PIMS"))

df_prod.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
       .saveAsTable("Bronze_LH.dbo.production_raw")
print(f"✅ Bronze_LH.production_raw: {df_prod.count()} rows written")

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

# One OPEX row per field per day BROKEN OUT across cost categories, plus occasional CAPEX events on
# fields with a capital project. Generated deterministically (seed=7) so the cost table is
# reproducible across runs. 8 fields x _SEED_DAYS x 10 categories keeps this well above 50k rows.
_cost_opex_base = {       # TOTAL daily OPEX baseline per field (USD), split across the categories below
    "Oseberg": 45000.0, "Troll": 62000.0, "Gullfaks": 38000.0, "Snorre": 19000.0,
    "Ekofisk": 71000.0, "Johan Sverdrup": 88000.0, "Grane": 27000.0, "Heidrun": 41000.0,
}
# (category department, share-of-daily-base) — shares sum to 1.0; each row uses a distinct department
_opex_categories = [
    ("Operations",   0.22), ("Utilities",    0.18), ("Maintenance", 0.15),
    ("Process",      0.10), ("Supply Chain", 0.09), ("HR",          0.08),
    ("Logistics",    0.06), ("HSE",          0.05), ("Subsea",      0.04),
    ("Digital",      0.03),
]
_field_vendor = {
    "Oseberg": "Schlumberger", "Troll": "Halliburton", "Gullfaks": "Baker Hughes",
    "Snorre": "Internal", "Ekofisk": "Weatherford", "Johan Sverdrup": "Schlumberger",
    "Grane": "Halliburton", "Heidrun": "Baker Hughes",
}
_capex_projects = {       # fields with an active capital project (others are OPEX-only)
    "Gullfaks": "GF-Expansion", "Snorre": "SN-Upgrade", "Johan Sverdrup": "JS-Phase2",
    "Heidrun": "HD-Subsea", "Ekofisk": "EK-Revamp",
}

random.seed(7)
cost_rows = []
_cid = 1
for _d in range(_SEED_DAYS):
    _day = _SEED_START + timedelta(days=_d)
    for _field, _base in _cost_opex_base.items():
        for _dept, _share in _opex_categories:
            cost_rows.append((f"C-{_cid:06d}", _day, _field, "OPEX",
                round(_base * _share * random.uniform(0.90, 1.10), 1),
                _dept, "PAB-2024", _field_vendor[_field]))
            _cid += 1
        if _field in _capex_projects and random.random() < 0.06:       # ~6% CAPEX event
            cost_rows.append((f"C-{_cid:06d}", _day, _field, "CAPEX",
                round(random.uniform(90000.0, 260000.0), 1),
                "Engineering", _capex_projects[_field], "Baker Hughes"))
            _cid += 1

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

# Generate 50,000 scheduled activities scattered across fields and dates within the seeding window.
# Deterministic (seed=13) so the schedule table is reproducible across runs.
_activities = ["Well Inspection", "Maintenance Shutdown", "Subsea Survey", "Chemical Treatment",
               "Production Test", "Safety Audit", "Equipment Upgrade", "Routine Inspection",
               "Well Stimulation", "Pipeline Integrity Check"]
_priorities = ["Critical", "High", "Medium", "Low"]
_statuses   = ["Planned", "In-Progress", "Completed"]
_teams      = ["Team Alpha", "Team Beta", "Team Gamma", "HSE Team", "External Vendor"]
_fields_list = list(_field_profiles.keys())

random.seed(13)
schedule_rows = []
for _i in range(1, 50001):
    _start = _SEED_START + timedelta(days=random.randint(0, _SEED_DAYS - 1))
    _end   = _start + timedelta(days=random.randint(2, 10))
    schedule_rows.append((f"S-{_i:06d}", _start, _end,
        random.choice(_activities), random.choice(_fields_list),
        random.choice(_priorities), random.choice(_statuses), random.choice(_teams)))

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
