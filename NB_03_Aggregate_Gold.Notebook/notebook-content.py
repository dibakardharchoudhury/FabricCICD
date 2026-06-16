# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "06434335-fbfd-4d2b-879d-4962bdd8d605",
# META       "default_lakehouse_name": "Gold_LH",
# META       "default_lakehouse_workspace_id": "292e18c3-b95e-42d1-bb02-9a2064fee5b8",
# META       "known_lakehouses": [
# META         {
# META           "id": "cee2ea93-ccb4-4bb3-8338-4a91840b9509"
# META         },
# META         {
# META           "id": "4207398e-6d15-4407-8d8d-cbb50d661ec6"
# META         },
# META         {
# META           "id": "06434335-fbfd-4d2b-879d-4962bdd8d605"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "86313016-e213-a285-4d09-9801e4f0072b",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# Fabric Notebook: NB_03_Aggregate_Gold
# Purpose: Aggregate Silver → Gold layer (report-ready, pre-computed KPIs)
# Layer: Gold — report-oriented, aggregated, enriched
#
# OWNERSHIP SPLIT (important):
#   • Gold_LH.production_daily  → produced by the DATAFLOW DF_Gold_PA (NOT this notebook).
#   • Gold_LH.cost_monthly      → produced here (Cell 3).
#   • Gold_LH.schedule_summary  → produced here (Cell 4).
#   • Gold_LH.field_kpi_facts   → produced here (Cell 5, joins production_daily + cost_monthly).
# This notebook never recreates production_daily; it only reads it for the cross-domain KPI join.
# Cell 6 then forces the lakehouse catalog/metadata sync so Gold_SM can resolve the new tables.
# ─────────────────────────────────────────────────────────────────────────────

# Cell 1 — Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, avg, count, max as _max, min as _min,
    round as _round, date_format, current_timestamp, lit
)
spark = SparkSession.builder.getOrCreate()
print("Starting Gold aggregation...")

# Cell 2 — Gold Production: OWNED BY THE DATAFLOW (DF_Gold_PA) — do NOT recreate here.
# DF_Gold_PA aggregates Silver_LH.production_conformed → Gold_LH.production_daily (Replace mode).
# Recreating it in this notebook would double-own the table and race the dataflow. Instead we just
# confirm it is present — DF_Gold_PA runs BEFORE NB_03 in PL_Refresh_Master — and read it later in
# Cell 5 for the cross-domain KPI join. (Cell 6 separately forces the catalog/metadata sync.)
if not spark.catalog.tableExists("Gold_LH.dbo.production_daily"):
    raise Exception(
        "Gold_LH.dbo.production_daily is missing. It is produced by the DF_Gold_PA dataflow, which "
        "must run BEFORE this notebook in PL_Refresh_Master. Check the pipeline ordering / dataflow run."
    )
print("✅ Gold_LH.production_daily present (produced by DF_Gold_PA) — not recreated by this notebook.")

# Cell 3 — Gold Cost: monthly field + cost type summary
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

# Cell 4 — Gold Schedule: field activity summary
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

# Cell 5 — Cross-domain enrichment: KPI fact table for reporting
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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 6 — Gold validation + force the lakehouse catalog/metadata sync before the model refresh.
# ---------------------------------------------------------------------------------------------
# WHY THIS EXISTS: the Gold Delta tables are written to OneLake SYNCHRONOUSLY — they are readable in
#   Spark, visible in the lakehouse, and queryable immediately. What lags is the LAKEHOUSE CATALOG /
#   SQL-ENDPOINT METADATA SYNC: for a few minutes after a table is (re)created, that metadata has not
#   caught up, so the Gold_SM refresh cannot resolve the newly created tables and fails with
#   0xC14700DF "source tables ... do not exist or access denied" — even though the data is physically
#   there. A filesystem check (notebookutils.fs.ls) is the WRONG tool for this: the files exist
#   instantly, so it would always pass and never actually guard the refresh.
#
# WHAT IT DOES: forces that metadata sync and WAITS for it to complete via
#   sempy_labs.refresh_sql_endpoint_metadata — the wrapper for the Fabric "Refresh SQL Endpoint
#   Metadata" API. It blocks until the lakehouse catalog has reconciled the new tables (up to the
#   timeout) and returns a status DataFrame. Only after that sync succeeds do we report row counts and
#   let PL_Refresh_Master proceed to the Gold_SM refresh. Workspace + Gold_LH ids are resolved AT
#   RUNTIME, so it is correct in Dev and in every deployed stage (fabric-cicd rebinds per workspace).
import sempy_labs as labs
import notebookutils
from datetime import datetime, timezone

_EXPECTED_TABLES = ["production_daily", "cost_monthly", "schedule_summary", "field_kpi_facts"]

# Resolve the workspace + default (Gold_LH) lakehouse this notebook is attached to — at runtime.
_ws_id = notebookutils.runtime.context.get("currentWorkspaceId") or spark.conf.get("trident.workspace.id")
_lh_id = notebookutils.runtime.context.get("defaultLakehouseId") or spark.conf.get("trident.lakehouse.id")
if not _ws_id or not _lh_id:
    raise Exception("Could not resolve the current workspace/Gold_LH ids — cannot sync lakehouse metadata.")

# Force the lakehouse SQL-endpoint / catalog metadata to sync and BLOCK until it completes (or 5 min).
# Time the call so the run log shows WHEN the catalog was reconciled and HOW LONG it took to block.
print("\n── Forcing lakehouse catalog/metadata sync so Gold_SM can see the new tables ──")
_t0 = datetime.now(timezone.utc)
print(f"   started : {_t0:%Y-%m-%d %H:%M:%S} UTC")
_sync = labs.refresh_sql_endpoint_metadata(
    item=_lh_id, type="Lakehouse", workspace=_ws_id,
    timeout_unit="Minutes", timeout_value=5,
)
_t1 = datetime.now(timezone.utc)
print(f"   finished: {_t1:%Y-%m-%d %H:%M:%S} UTC  (blocked {(_t1 - _t0).total_seconds():.1f}s)")

# Pretty-print just the columns that matter, one aligned row per table, instead of dumping the
# raw wide DataFrame. Per-table Status meaning (the API call ALWAYS runs and blocks to completion;
# Status reports what each table needed):
#   Success = re-synced this run (metadata was stale)   NotRun = already current (healthy, no work)
#   Failure = the sync FAILED for that table (blocks the downstream Gold_SM refresh).
def _col(df, *names):
    for _n in names:
        if hasattr(df, "columns") and _n in df.columns:
            return _n
    return None

_name_col   = _col(_sync, "Table Name", "TableName", "table_name", "Name")
_status_col = _col(_sync, "Status", "status")
_counts = {"Success": 0, "NotRun": 0, "Failure": 0, "Failed": 0}
_failed_tables = []
if _name_col:
    _icon = {"Success": "✓", "NotRun": "•", "Failure": "✗", "Failed": "✗"}
    _rows = []
    for _, _r in _sync.iterrows():
        _nm  = str(_r[_name_col]).split(".")[-1]
        _st  = str(_r[_status_col]) if _status_col else ""
        if _st in _counts:
            _counts[_st] += 1
        if _st in ("Failure", "Failed"):
            _failed_tables.append(_nm)
        _rows.append((_icon.get(_st, "·"), _nm, _st))
    _w = max((len(_n) for _, _n, _ in _rows), default=10)
    for _ic, _nm, _st in sorted(_rows, key=lambda x: x[1]):
        print(f"   {_ic} {_nm.ljust(_w)}  {_st}")
    # Plain-language verdict so the run log answers "did they succeed?" without decoding statuses.
    _resynced = _counts["Success"]
    _current  = _counts["NotRun"]
    _failures = _counts["Failure"] + _counts["Failed"]
    print(f"   legend: ✓ re-synced  • already current  ✗ failed")
    print(f"   result: {_resynced} re-synced, {_current} already current, {_failures} failed "
          f"— all reconciled as of {_t1:%H:%M:%S} UTC")
else:
    print(_sync)

# Defensive check: if the status DataFrame exposes a table-name column, confirm every expected Gold
# table is present in the synced metadata before allowing the downstream refresh to proceed. The
# "Table Name" column is schema-qualified (e.g. "dbo.production_daily"), so strip the schema prefix
# before comparing against the bare names in _EXPECTED_TABLES.
_synced = set()
for _c in ("Table Name", "TableName", "table_name", "Name"):
    if hasattr(_sync, "columns") and _c in _sync.columns:
        _synced = {str(_v).split(".")[-1] for _v in _sync[_c]}
        break

# Guard 1: any table whose sync explicitly FAILED must stop the pipeline — a Direct-Lake refresh
# against a failed table would error with 0xC14700DF.
if _failed_tables:
    raise Exception(
        f"Lakehouse metadata sync FAILED for: {sorted(set(_failed_tables))}. "
        f"A Direct-Lake refresh of Gold_SM would fail with 0xC14700DF. Investigate the SQL endpoint."
    )

# Guard 2: every expected Gold table must appear in the synced metadata.
_missing = (set(_EXPECTED_TABLES) - _synced) if _synced else set()
if _missing:
    raise Exception(
        f"Gold tables still not present in the synced lakehouse metadata after 5 min: {sorted(_missing)}. "
        f"A Direct-Lake refresh would fail with 0xC14700DF. Investigate the lakehouse metadata sync."
    )

# Metadata is synced — now report final row counts for the run log.
print("\n── Gold Layer Validation ────────────────────────────")
for _tbl in _EXPECTED_TABLES:
    print(f"  Gold_LH.{_tbl}: {spark.table(f'Gold_LH.dbo.{_tbl}').count()} rows")

print(f"\n✅ Lakehouse metadata synced for {sorted(_EXPECTED_TABLES)} — safe to refresh Gold_SM.")
print("🏆 Gold aggregation COMPLETE — tables ready for Semantic Model")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
