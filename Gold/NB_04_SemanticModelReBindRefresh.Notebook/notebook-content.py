# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "00000000-0000-0000-0000-000000000000",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# Validate Gold, rebind, and refresh the Direct Lake model.
refresh_semantic_model = True

from sempy.fabric._client._rest_client import PowerBIRestClient


def _powerbi_base_url(self):
    return "https://api.powerbi.com/"


PowerBIRestClient._get_default_base_url = _powerbi_base_url

import sempy_labs as labs
from sempy_labs import directlake
import notebookutils
import time

_EXPECTED_TABLES = ["production_daily", "cost_monthly", "schedule_summary", "field_kpi_facts"]

_ws_id = notebookutils.runtime.context.get("currentWorkspaceId") or spark.conf.get("trident.workspace.id")
if not _ws_id:
    raise Exception("Could not resolve the current workspace id - cannot refresh Gold_SM.")

print("\n-- Gold Layer Validation --------------------------------")
for _tbl in _EXPECTED_TABLES:
    print(f"  Gold_LH.{_tbl}: {spark.table(f'Gold_LH.dbo.{_tbl}').count()} rows")

_SEMANTIC_MODEL = "Gold_SM"
_MAX_ATTEMPTS = 15
_BACKOFF_SECS = 120

_GOLD_LAKEHOUSE = "Gold_LH"
_gold_lh_meta = notebookutils.lakehouse.get(_GOLD_LAKEHOUSE, _ws_id)
_gold_lh_id = _gold_lh_meta["id"] if isinstance(_gold_lh_meta, dict) else _gold_lh_meta.id
directlake.update_direct_lake_model_connection(
    dataset=_SEMANTIC_MODEL,
    workspace=_ws_id,
    source=_gold_lh_id,
    source_type="Lakehouse",
    source_workspace=_ws_id,
    use_sql_endpoint=False,
)
print(
    f"'{_SEMANTIC_MODEL}' Direct Lake connection re-pointed to '{_GOLD_LAKEHOUSE}' "
    f"({_gold_lh_id}) in this workspace before refresh."
)

if refresh_semantic_model:
    print(f"\n-- Refreshing Direct Lake model '{_SEMANTIC_MODEL}' (full reframe) --")
    print(f"Semantic Link Power BI endpoint: {PowerBIRestClient().default_base_url}")

_refreshed = not refresh_semantic_model
for _attempt in range(1, _MAX_ATTEMPTS + 1) if refresh_semantic_model else ():
    try:
        labs.refresh_semantic_model(
            dataset=_SEMANTIC_MODEL,
            workspace=_ws_id,
            refresh_type="full",
        )
        print(
            f"'{_SEMANTIC_MODEL}' refreshed on attempt {_attempt}/{_MAX_ATTEMPTS}; "
            "the report is up to date."
        )
        _refreshed = True
        break
    except Exception as _error:
        _message = str(_error)
        _forbidden = "403 Forbidden" in _message
        _transient = not _forbidden and (
            "0xC14700DF" in _message or "do not exist or access" in _message.lower()
        )
        if _transient and _attempt < _MAX_ATTEMPTS:
            print(
                f"Attempt {_attempt}/{_MAX_ATTEMPTS}: tables are still synchronizing; "
                f"retrying in {_BACKOFF_SECS} seconds."
            )
            time.sleep(_BACKOFF_SECS)
            continue

        _failure_guidance = (
            "The Power BI API returned a non-retryable 403. Verify that this notebook and its "
            "pipeline were published by the required administrator user."
            if _forbidden
            else "The Direct Lake metadata synchronization did not complete within the retry window."
        )
        raise Exception(
            f"Refresh of '{_SEMANTIC_MODEL}' failed after {_attempt} attempt(s): {_error}. "
            f"{_failure_guidance}"
        ) from _error

if not _refreshed:
    raise RuntimeError(f"Refresh of '{_SEMANTIC_MODEL}' did not complete")

print("Gold semantic model rebind and refresh complete")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }