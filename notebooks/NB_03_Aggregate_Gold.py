# Fabric Notebook: NB_03_Aggregate_Gold
# Purpose: Aggregate Silver → Gold layer (report-ready, pre-computed KPIs)
# Layer: Gold — report-oriented, aggregated, enriched
#
# ⚠ IMPORTANT — The Gold layer is completed by TWO artifacts working together:
#   1. Dataflow Gen2  DF_Gold_PA  → builds  Gold_LH.production_daily
#      (the production aggregation — a low-code Power Query step, owned by DF2)
#   2. This notebook  NB_03       → builds  cost_monthly, schedule_summary,
#      and the cross-domain  field_kpi_facts  (which JOINS production_daily).
#
# This notebook intentionally does NOT build production_daily. It depends on
# DF_Gold_PA having run first (run it manually before NB_03, or add it as a
# Dataflow activity ahead of NB_03 in your refresh pipeline). If production_daily
# is missing, the dependency check in the KPI cell stops with a clear message.
# ─────────────────────────────────────────────────────────────────────────────

# Cell 1 — Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, avg, count, max as _max, min as _min,
    round as _round, date_format, current_timestamp, lit
)
spark = SparkSession.builder.getOrCreate()
print("Starting Gold aggregation (cost + schedule + cross-domain KPIs)...")
print("Note: production_daily is produced by Dataflow Gen2 DF_Gold_PA, not this notebook.")

# Cell 2 — Gold Cost: monthly field + cost type summary
df_silver_cost = spark.table("Silver_LH.dbo.cost_conformed")

df_gold_cost = (
    df_silver_cost
    .withColumn("year_month", date_format(col("date"), "yyyy-MM"))
    .groupBy("year_month", "field", "cost_type")
    .agg(
        _sum("amount_usd").alias("total_cost_usd"),
        avg("cost_per_boe").alias("avg_cost_per_boe"),
        count("cost_id").alias("transaction_count"),
        _max("amount_usd").alias("max_single_cost_usd"),
    )
    .withColumn("total_cost_usd",       _round("total_cost_usd", 2))
    .withColumn("avg_cost_per_boe",     _round("avg_cost_per_boe", 4))
    .withColumn("report_generated_at",  current_timestamp())
)

df_gold_cost.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("Gold_LH.dbo.cost_monthly")

print(f"✅ Gold_LH.cost_monthly: {df_gold_cost.count()} rows")

# Cell 3 — Gold Schedule: field activity summary
df_silver_sched = spark.table("Silver_LH.dbo.schedule_conformed")

df_gold_sched = (
    df_silver_sched
    .groupBy("field", "priority", "status")
    .agg(
        count("schedule_id").alias("activity_count"),
        avg("duration_days").alias("avg_duration_days"),
        _sum(col("is_critical").cast("int")).alias("critical_count"),
        _sum(col("is_in_progress").cast("int")).alias("in_progress_count"),
    )
    .withColumn("avg_duration_days",    _round("avg_duration_days", 1))
    .withColumn("report_generated_at",  current_timestamp())
)

df_gold_sched.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("Gold_LH.dbo.schedule_summary")

print(f"✅ Gold_LH.schedule_summary: {df_gold_sched.count()} rows")

# Cell 4 — Cross-domain enrichment: KPI fact table for reporting
# This step JOINS production_daily, which is produced by Dataflow Gen2 DF_Gold_PA.
# Guard: fail fast with a clear message if DF_Gold_PA has not been run yet.
if not spark.catalog.tableExists("Gold_LH.dbo.production_daily"):
    raise RuntimeError(
        "Gold_LH.production_daily is missing. Run the Dataflow Gen2 'DF_Gold_PA' "
        "before this notebook (in PL_Refresh_Master, DF_Gold_PA runs first). "
        "DF_Gold_PA owns the production aggregation — it is required to complete the Gold layer."
    )

df_prod_summary = (
    spark.table("Gold_LH.dbo.production_daily")
    .groupBy("field")
    .agg(
        avg("total_boe").alias("avg_daily_boe"),
        _sum("total_oil_bbl").alias("cumulative_oil_bbl"),
        avg("active_well_count").alias("avg_active_wells"),
    )
)

df_cost_summary = (
    spark.table("Gold_LH.dbo.cost_monthly")
    .groupBy("field")
    .agg(
        _sum("total_cost_usd").alias("total_cost_usd"),
        avg("avg_cost_per_boe").alias("avg_cost_per_boe"),
    )
)

df_kpi_facts = (
    df_prod_summary
    .join(df_cost_summary, "field", "left")
    .withColumn("avg_daily_boe",       _round("avg_daily_boe", 2))
    .withColumn("cumulative_oil_bbl",  _round("cumulative_oil_bbl", 2))
    .withColumn("total_cost_usd",      _round("total_cost_usd", 2))
    .withColumn("avg_cost_per_boe",    _round("avg_cost_per_boe", 4))
    .withColumn("report_generated_at", current_timestamp())
)

df_kpi_facts.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("Gold_LH.dbo.field_kpi_facts")

print(f"✅ Gold_LH.field_kpi_facts: {df_kpi_facts.count()} rows")
display(df_kpi_facts)

# Cell 5 — Final validation
print("\n── Gold Layer Validation ──────────────────")
print("  Gold_LH.production_daily   ← produced by Dataflow Gen2 DF_Gold_PA")
print("  Gold_LH.cost_monthly       ← produced by this notebook")
print("  Gold_LH.schedule_summary   ← produced by this notebook")
print("  Gold_LH.field_kpi_facts    ← produced by this notebook (joins production_daily)")
print("")
for tbl in ["production_daily", "cost_monthly", "schedule_summary", "field_kpi_facts"]:
    n = spark.table(f"Gold_LH.dbo.{tbl}").count()
    print(f"  Gold_LH.{tbl}: {n} rows")

print("\n🏆 Gold aggregation COMPLETE — DF_Gold_PA + NB_03 together built the Gold layer.")
print("   Tables are ready for the Semantic Model.")
