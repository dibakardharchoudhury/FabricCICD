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
# Cell 6 then reframes the Direct Lake (on OneLake) model Gold_SM so it picks up the new data.
# ─────────────────────────────────────────────────────────────────────────────

# Cell 1 — Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, avg, count, countDistinct, max as _max, min as _min,
    round as _round, date_format, current_timestamp, lit
)
spark = SparkSession.builder.getOrCreate()
print("Starting Gold aggregation...")

# Helper — write a Gold Delta table WITHOUT dropping/recreating it on every run.
# WHY: Gold_SM is Direct Lake (on OneLake). For ~10 min after a table is DROPPED-AND-RECREATED, its
# refresh fails with 0xC14700DF because OneLake / the lakehouse catalog must rediscover the new table
# OBJECT (new identity). Using overwriteSchema=true on every run forces exactly that recreate — that is
# the lag seen in the refresh history. Instead: if the table already exists, overwrite the DATA ONLY
# (table identity preserved → no rediscovery → the model reframes immediately). Only use overwriteSchema
# on first creation or genuine schema drift (AnalysisException fallback).
def _write_gold(df, table):
    full = f"Gold_LH.dbo.{table}"
    if spark.catalog.tableExists(full):
        try:
            df.write.format("delta").mode("overwrite").saveAsTable(full)   # data-only, identity kept
            return
        except Exception:
            pass  # schema changed since last run → fall through and recreate with the new schema
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full)

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

_write_gold(df_gold_cost, "cost_monthly")

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

_write_gold(df_gold_sched, "schedule_summary")

print(f"✅ Gold_LH.schedule_summary: {df_gold_sched.count()} rows")

# Cell 5 — Cross-domain enrichment: KPI fact table for reporting (one row per field).
# OUTPUT SCHEMA IS FIXED by Gold_SM: field, avg_daily_boe, cumulative_oil_bbl, avg_active_wells,
# total_cost_usd, avg_cost_per_boe. Do NOT change those output columns.
#
# production_daily is OWNED by DF_Gold_PA and normally carries the AGGREGATED schema (date, field,
# total_oil_bbl, total_boe, active_well_count, ...). But if DF_Gold_PA has not refreshed it this run
# (e.g. its Silver source params are unset), the table can still hold stale ROW-LEVEL Silver columns
# (well_id, date, field, oil_bbl, boe_total, ...). Read it schema-adaptively so this cell produces the
# SAME per-field KPIs either way instead of failing with UNRESOLVED_COLUMN on total_boe.
_df_prod = spark.table("Gold_LH.dbo.production_daily")
_prod_cols = set(_df_prod.columns)

if {"total_boe", "total_oil_bbl", "active_well_count"} <= _prod_cols:
    # Aggregated (DF_Gold_PA) schema — already one row per (date, field).
    df_prod_summary = (
        _df_prod
        .groupBy("field")
        .agg(
            avg("total_boe").alias("avg_daily_boe"),
            _sum("total_oil_bbl").alias("cumulative_oil_bbl"),
            avg("active_well_count").alias("avg_active_wells"),
        )
    )
else:
    # Stale row-level (Silver) schema — first roll up to daily field grain, then to per-field KPIs so
    # the numbers match what the aggregated schema would have produced.
    _daily = (
        _df_prod
        .groupBy("field", "date")
        .agg(
            _sum("boe_total").alias("_daily_boe"),
            _sum("oil_bbl").alias("_daily_oil"),
            countDistinct("well_id").alias("_active_wells"),
        )
    )
    df_prod_summary = (
        _daily
        .groupBy("field")
        .agg(
            avg("_daily_boe").alias("avg_daily_boe"),
            _sum("_daily_oil").alias("cumulative_oil_bbl"),
            avg("_active_wells").alias("avg_active_wells"),
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

_write_gold(df_kpi_facts, "field_kpi_facts")

print(f"✅ Gold_LH.field_kpi_facts: {df_kpi_facts.count()} rows")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 6 — Gold validation + reframe the Direct Lake (on OneLake) model after the write.
# ---------------------------------------------------------------------------------------------
# WHY THIS EXISTS: Gold_SM is DIRECT LAKE *ON ONELAKE* (its data source is AzureStorage.DataLake
#   against the Gold_LH OneLake path), so at refresh time it FRAMES the Delta tables DIRECTLY from
#   OneLake — it does NOT resolve them through the SQL analytics endpoint. (Per MS docs, Direct Lake
#   on OneLake "doesn't fall back to DirectQuery via the SQL analytics endpoint"; the SQL endpoint is
#   only in the discovery/permission path for Direct Lake *on SQL*.) So a SQL-endpoint metadata sync
#   is a no-op for THIS model's refresh and has been removed — it only ever helped T-SQL/SQL-endpoint
#   consumers and the authoring table picker, neither of which this refresh uses.
#
# WHAT ACTUALLY GUARANTEES A CLEAN REFRESH: two things, both already in place —
#   1) Cell 1 _write_gold() overwrites the Gold tables DATA-ONLY (identity preserved) instead of
#      dropping-and-recreating them, so there is no new table object for OneLake/the lakehouse catalog
#      to rediscover — the ~10-min 0xC14700DF window is eliminated at the source.
#   2) The FULL REFRESH at the end of this cell: NB_03 reframes Gold_SM itself, immediately after the
#      write, under the same identity that wrote the tables — so the model is authoritative the moment
#      this notebook finishes and never depends on a later manual/scheduled refresh hitting a stale frame.
#
# THE FAILURE MODE — METADATA-SYNC LAG WHEN TABLES ARE CREATED FROM SCRATCH (handled by the RETRY
# LOOP with a LONG window). The first time this runs in a stage (e.g. a fresh fabric-cicd deploy to
# ws-CICD-PROD) the Gold_LH tables do not exist yet, so _write_gold() CREATES them from scratch. A
# brand-new Delta table object has to be registered and synced into the lakehouse/OneLake metadata
# before Direct Lake framing can resolve it, and that first-creation sync can take MANY MINUTES
# (observed ~10 min, sometimes longer for the freshest table, usually field_kpi_facts). Until the sync
# completes, framing returns 0xC14700DF "source tables ... do not exist or access denied" — the wording
# mentions access, but this is NOT a permission/ownership problem: the SAME identity with the SAME
# permissions succeeds once the metadata has caught up. (Proven repeatedly: a failing run reruns
# cleanly minutes later with zero changes; a genuine owner-access error would be permanent.) So the fix
# is to RETRY the reframe with backoff over a window LONG ENOUGH to outlast the create-from-scratch
# sync, instead of taking over ownership or failing the run. On STEADY-STATE runs the tables already
# exist and are overwritten DATA-ONLY (identity preserved), so there is nothing to re-register and the
# first attempt normally succeeds immediately.
import sempy_labs as labs
from sempy_labs import directlake
import notebookutils
import time

_EXPECTED_TABLES = ["production_daily", "cost_monthly", "schedule_summary", "field_kpi_facts"]

# Resolve the workspace this notebook is attached to — at runtime, so it is correct in Dev and in
# every deployed stage (fabric-cicd rebinds per workspace).
_ws_id = notebookutils.runtime.context.get("currentWorkspaceId") or spark.conf.get("trident.workspace.id")
if not _ws_id:
    raise Exception("Could not resolve the current workspace id — cannot refresh Gold_SM.")

# Report final row counts for the run log.
print("\n── Gold Layer Validation ────────────────────────────")
for _tbl in _EXPECTED_TABLES:
    print(f"  Gold_LH.{_tbl}: {spark.table(f'Gold_LH.dbo.{_tbl}').count()} rows")

# ── Refresh Gold_SM HERE, under the identity that just wrote the tables ──────────────────────
# This is the authoritative guarantee (see the cell header). Gold_SM is Direct Lake ON ONELAKE, so
# it frames the Delta tables straight from OneLake; a FULL refresh reframes every Direct Lake
# partition and makes the model pick up the just-written field_kpi_facts (and the other tables)
# atomically. We RETRY with backoff over a LONG window so that on a first run — where the tables are
# CREATED FROM SCRATCH and need a slow metadata sync before framing can resolve them (see header) —
# the loop simply waits the sync out instead of failing. NOTE: this only TRIGGERS a refresh; it does
# NOT change the model, its measures, the calc table, or the report.
_SEMANTIC_MODEL = "Gold_SM"
# Window sized to outlast the create-from-scratch metadata sync (observed ~10 min, allow margin):
# 15 attempts × 120s ≈ 28 min of total wait. Steady-state runs (tables already exist) succeed on
# attempt 1, so this big ceiling only ever costs time on the very first run in a new stage.
_MAX_ATTEMPTS   = 15         # total tries
_BACKOFF_SECS   = 120        # wait between tries (create-from-scratch sync can run ~10 min+)

# ── Re-point Gold_SM at THIS workspace's Gold_LH BEFORE refreshing (self-healing rebind) ─────
# WHY: Gold_SM is published from Git with its Direct Lake source path baked to the ORIGINAL Dev
# workspace/lakehouse GUID (.../<devWorkspaceId>/<devGoldLhId>). parameter.yml deliberately does NOT
# rewrite the SemanticModel, so the ONLY thing that re-points it per stage is a code rebind. If that
# rebind never ran for this workspace (NB_04 Cell 6b skipped, or the repo was Git-synced straight
# into this workspace), the refresh below frames against a lakehouse GUID that does not exist here and
# fails with "The Fabric artifact '<devGoldLhId>' is not found, or you do not have permission...".
# Rebinding to the SAME-named Gold_LH in the CURRENT workspace (resolved at runtime) makes this
# notebook self-healing and independent of how it was deployed. Idempotent: if the model already
# points at this workspace's Gold_LH it is effectively a no-op. Needs workspace Admin/Member — the
# same right the refresh already requires. use_sql_endpoint=False = Direct Lake OVER ONELAKE.
_GOLD_LAKEHOUSE = "Gold_LH"
_gold_lh_meta = notebookutils.lakehouse.get(_GOLD_LAKEHOUSE, _ws_id)
_gold_lh_id   = _gold_lh_meta["id"] if isinstance(_gold_lh_meta, dict) else _gold_lh_meta.id
directlake.update_direct_lake_model_connection(
    dataset=_SEMANTIC_MODEL, workspace=_ws_id,
    source=_gold_lh_id, source_type="Lakehouse",
    source_workspace=_ws_id, use_sql_endpoint=False,
)
print(f"🔗 '{_SEMANTIC_MODEL}' Direct Lake connection re-pointed to '{_GOLD_LAKEHOUSE}' "
      f"({_gold_lh_id}) in this workspace before refresh.")

print(f"\n── Refreshing Direct Lake (on OneLake) model '{_SEMANTIC_MODEL}' (full reframe) ──")
_refreshed = False
for _attempt in range(1, _MAX_ATTEMPTS + 1):
    try:
        labs.refresh_semantic_model(
            dataset=_SEMANTIC_MODEL, workspace=_ws_id, refresh_type="full",
        )
        print(f"✅ '{_SEMANTIC_MODEL}' refreshed on attempt {_attempt}/{_MAX_ATTEMPTS} "
              f"(Direct Lake reframe complete) — report is up to date.")
        _refreshed = True
        break
    except Exception as _e:
        _msg = str(_e)
        # A newly-created table not yet synced into metadata surfaces as this text. It is transient
        # and clears once the create-from-scratch sync completes — so keep retrying.
        _transient = ("0xC14700DF" in _msg) or ("do not exist or access" in _msg.lower())
        if _transient and _attempt < _MAX_ATTEMPTS:
            print(f"⏳ Attempt {_attempt}/{_MAX_ATTEMPTS}: tables still syncing into metadata after "
                  f"create-from-scratch; retrying in {_BACKOFF_SECS}s "
                  f"(elapsed wait so far ~{(_attempt - 1) * _BACKOFF_SECS // 60} min)...")
            time.sleep(_BACKOFF_SECS)
            continue
        # Out of retries, or an error that is not the metadata-sync lag — surface it precisely.
        raise Exception(
            f"Refresh of Direct Lake (on OneLake) model '{_SEMANTIC_MODEL}' FAILED after "
            f"{_attempt} attempt(s) (~{((_attempt - 1) * _BACKOFF_SECS) // 60} min of waiting): {_e}\n"
            f"  • 0xC14700DF / 'do not exist or access' here means the create-from-scratch metadata sync\n"
            f"    still had not completed within the retry window. This is NOT a permission/ownership\n"
            f"    issue — the same identity refreshes cleanly once the sync finishes. Raise _MAX_ATTEMPTS\n"
            f"    and/or _BACKOFF_SECS above to extend the window if a stage's first-run sync is slower.\n"
            f"  • Re-running this notebook (or the pipeline) after a few minutes will also succeed, since\n"
            f"    by then the tables are fully registered."
        ) from _e

print("🏆 Gold aggregation COMPLETE — tables written, Gold_SM refreshed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
