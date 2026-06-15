# Fabric notebook source


# CELL ********************

# Fabric Notebook: NB_02_Transform_Silver
# Purpose: Clean, conform, and enrich Bronze tables → Silver layer
# Layer: Silver — domain-oriented, de-duplicated, typed
# This is the PRIMARY notebook for the CI/CD change demo
# ─────────────────────────────────────────────────────────────────────────────
# DEMO CHANGE: When demonstrating Git CI/CD, add the gor_ratio KPI in Cell 2
# and commit — reviewers will see the exact code change in GitHub
# ─────────────────────────────────────────────────────────────────────────────

# Cell 1 — Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, avg, count, when, lit, current_timestamp,
    datediff, to_date, round as _round, coalesce
)
spark = SparkSession.builder.getOrCreate()
print("Starting Silver transformation...")

# Cell 2 — Production: filter, cast, compute KPIs
df_prod_raw = spark.table("Bronze_LH.dbo.production_raw")

df_silver_prod = (
    df_prod_raw
    .filter(col("status") != "Shut-in")                         # Exclude shut-in wells
    .withColumn("oil_bbl",       col("oil_bbl").cast("double"))
    .withColumn("gas_mcf",       col("gas_mcf").cast("double"))
    .withColumn("water_bbl",     col("water_bbl").cast("double"))
    # ── KPIs ──────────────────────────────────────────────────────────────────
    .withColumn("boe_total",      _round(col("oil_bbl") + col("gas_mcf") * 0.17, 2))
    .withColumn("water_cut_pct",  _round(
        col("water_bbl") / (col("oil_bbl") + col("water_bbl") + 0.001) * 100, 2
    ))
    # ── DEMO CI/CD CHANGE — add gor_ratio ─────────────────────────────────────
    # When demonstrating CI/CD, uncomment the line below, run, and commit to Git
    # .withColumn("gor_ratio",  _round(col("gas_mcf") / (col("oil_bbl") + 0.001), 3))
    # ──────────────────────────────────────────────────────────────────────────
    .withColumn("is_active",  col("status") == "Active")
    .withColumn("transformed_at", current_timestamp())
    .drop("ingested_at", "source_system")
)

df_silver_prod.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("Silver_LH.dbo.production_conformed")

count_prod = df_silver_prod.count()
print(f"✅ Silver_LH.production_conformed: {count_prod} rows")
display(df_silver_prod.limit(5))

# Cell 3 — Cost: enrich with daily BOE for cost-per-BOE KPI
df_cost_raw = spark.table("Bronze_LH.dbo.cost_raw")
df_prod_silver = spark.table("Silver_LH.dbo.production_conformed")

# Daily BOE by field for joining
daily_boe = (
    df_prod_silver
    .groupBy("field", "date")
    .agg(_sum("boe_total").alias("daily_boe"))
)

df_silver_cost = (
    df_cost_raw
    .join(daily_boe, ["field", "date"], "left")
    .withColumn("daily_boe",     coalesce(col("daily_boe"), lit(0.0)))
    .withColumn("cost_per_boe",  _round(col("amount_usd") / (col("daily_boe") + 0.001), 4))
    .withColumn("is_capex",      col("cost_type") == "CAPEX")
    .withColumn("transformed_at", current_timestamp())
    .drop("ingested_at", "source_system")
)

df_silver_cost.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("Silver_LH.dbo.cost_conformed")

print(f"✅ Silver_LH.cost_conformed: {df_silver_cost.count()} rows")

# Cell 4 — Schedule: add derived columns, flag criticals
df_sched_raw = spark.table("Bronze_LH.dbo.schedule_raw")

df_silver_sched = (
    df_sched_raw
    .withColumn("duration_days",   datediff(col("end_date"), col("start_date")))
    .withColumn("is_critical",     col("priority") == "Critical")
    .withColumn("is_in_progress",  col("status") == "In-Progress")
    .withColumn("priority_rank",   when(col("priority") == "Critical", 1)
                                  .when(col("priority") == "High",     2)
                                  .when(col("priority") == "Medium",   3)
                                  .otherwise(4))
    .withColumn("transformed_at",  current_timestamp())
    .drop("ingested_at", "source_system")
)

df_silver_sched.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("Silver_LH.dbo.schedule_conformed")

print(f"✅ Silver_LH.schedule_conformed: {df_silver_sched.count()} rows")

# Cell 5 — Validate row counts
print("\n── Silver Layer Validation ──────────────────────────")
for tbl in ["production_conformed", "cost_conformed", "schedule_conformed"]:
    n = spark.table(f"Silver_LH.dbo.{tbl}").count()
    print(f"  Silver_LH.{tbl}: {n} rows")

print("\n🏆 Silver transformation COMPLETE")
print("Next: Run NB_03_Aggregate_Gold")
