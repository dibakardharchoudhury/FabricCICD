# Fabric Notebook: NB_04_Bind_Stage_Config
# Purpose: ONE notebook that re-points every stage-specific binding to the CURRENT
#   stage's items, using the Variable Library 'VL_CICD_Bindings' as the single source
#   of truth and Fabric REST APIs (semantic-link-labs wrappers + raw REST) to push them.
#
# This is "Option 3" — it REPLACES ALL THREE deployment rules (notebook default
#   lakehouse, dataflow parameters, semantic-model connection) with config-as-code.
#   Run it as the FIRST activity of PL_Refresh_Master in EVERY stage, right after the
#   deployment-pipeline deploy. Because it reads the ACTIVE Variable Library value set,
#   the same code resolves Dev IDs in Dev and Prod IDs in Prod — no hardcoded GUIDs.
#
# It also CREATES the Variable Library itself on first run (idempotent bootstrap), so the
#   whole CI/CD binding layer — library + all three bindings — is provisioned from here.
#
# What it does (all binding values come from VL_CICD_Bindings, never hardcoded):
#   0. Variable Library VL_CICD_Bindings -> created if missing (4 Guid vars, Dev/Prod sets)
#   1. Notebooks NB_01/02/03   -> default lakehouse = Bronze/Silver/Gold (this stage)
#   2. Dataflow  DF_Gold_PA    -> parameters  SilverWorkspaceId / SilverLakehouseId
#   3. Semantic model Gold_SM  -> Direct Lake ON ONELAKE connection -> Gold_LH (this stage)
#
# Why this works for Gold_SM: a *data-source deployment rule* is NOT supported for
#   Direct Lake on OneLake (MS docs). update_direct_lake_model_connection() regenerates
#   the model's connection expression in code instead — use_sql_endpoint=False selects
#   Direct Lake OVER ONELAKE (not the SQL endpoint). So no parameter rule / ownership
#   dance in the pipeline is needed — but you must RUN this notebook as an identity that
#   can write to Gold_SM (own it / take ownership once if a different identity deployed).
#
# No default lakehouse required: this notebook only makes CONTROL-PLANE calls (Variable
#   Library + item-definition REST + Direct Lake connection). It never reads/writes Spark
#   tables, so it can run with NO lakehouse attached. Every sempy_labs call passes
#   workspace=WORKSPACE_ID explicitly, so nothing is inferred from an attached lakehouse.
#
# Prereqs: the running identity is a workspace Member/Admin and owns Gold_SM. The Variable
#   Library is created here on first run; the deployment pipeline activates the matching
#   value set per stage so consumers resolve the right IDs automatically.
# -----------------------------------------------------------------------------------

# Cell 1 — PARAMETERS (tag this cell as "parameters" in the Fabric notebook).
# These seed the Variable Library the FIRST time this notebook runs. After the library
# exists they are ignored — the library becomes the single source of truth and the
# deployment pipeline activates the right value set per stage. Fill in the real GUIDs
# once (or override at runtime from the pipeline) so the bootstrap can create the library.
dev_workspace_id         = "<dev-workspace-id>"
dev_bronze_lakehouse_id  = "<dev-bronze-lakehouse-id>"
dev_silver_lakehouse_id  = "<dev-silver-lakehouse-id>"
dev_gold_lakehouse_id    = "<dev-gold-lakehouse-id>"

prod_workspace_id        = "<prod-workspace-id>"
prod_bronze_lakehouse_id = "<prod-bronze-lakehouse-id>"
prod_silver_lakehouse_id = "<prod-silver-lakehouse-id>"
prod_gold_lakehouse_id   = "<prod-gold-lakehouse-id>"

# Cell 2 — Imports
%pip install -q semantic-link-labs
import json, base64, re, requests
import sempy_labs as labs
from sempy_labs import directlake
from sempy_labs import variable_library as vlib
import notebookutils

VL_NAME = "VL_CICD_Bindings"

# Cell 3 — Create the Variable Library IF it does not already exist (idempotent bootstrap)
# 4 Guid variables; Development value set is the default, Production overrides each value.
# The deployment pipeline activates 'Production' in the Prod stage so consumers resolve
# Prod IDs there automatically. Re-running this notebook simply skips creation.
existing = vlib.list_variable_libraries()
_name_col = next((c for c in existing.columns if "name" in c.lower()), None)
_already_exists = _name_col is not None and VL_NAME in existing[_name_col].tolist()
if not _already_exists:
    variables = [
        {"name": "WorkspaceId",       "type": "Guid", "value": dev_workspace_id,
         "note": "Current-stage workspace; all three lakehouses + DF/SM live here."},
        {"name": "BronzeLakehouseId", "type": "Guid", "value": dev_bronze_lakehouse_id,
         "note": "Bronze_LH id -> NB_01 default lakehouse."},
        {"name": "SilverLakehouseId", "type": "Guid", "value": dev_silver_lakehouse_id,
         "note": "Silver_LH id -> NB_02 default lakehouse + DF_Gold_PA SilverLakehouseId."},
        {"name": "GoldLakehouseId",   "type": "Guid", "value": dev_gold_lakehouse_id,
         "note": "Gold_LH id -> NB_03 default lakehouse + Gold_SM Direct Lake source."},
    ]
    value_sets = [
        {"name": "Development", "variableOverrides": [
            {"name": "WorkspaceId",       "value": dev_workspace_id},
            {"name": "BronzeLakehouseId", "value": dev_bronze_lakehouse_id},
            {"name": "SilverLakehouseId", "value": dev_silver_lakehouse_id},
            {"name": "GoldLakehouseId",   "value": dev_gold_lakehouse_id},
        ]},
        {"name": "Production", "variableOverrides": [
            {"name": "WorkspaceId",       "value": prod_workspace_id},
            {"name": "BronzeLakehouseId", "value": prod_bronze_lakehouse_id},
            {"name": "SilverLakehouseId", "value": prod_silver_lakehouse_id},
            {"name": "GoldLakehouseId",   "value": prod_gold_lakehouse_id},
        ]},
    ]
    vlib.create_variable_library(
        name=VL_NAME,
        variables=variables,
        value_sets=value_sets,
        value_sets_order=["Development", "Production"],
        description="CI/CD stage bindings consumed by notebooks, DF_Gold_PA, and (via this notebook) Gold_SM.",
    )
    print(f"✅ Created Variable Library '{VL_NAME}' (Development default, Production override).")
else:
    print(f"ℹ Variable Library '{VL_NAME}' already exists — leaving it as the source of truth.")

# Cell 4 — Read the ACTIVE Variable Library value set (single source of truth)
# Notebooks are a Variable Library consumer via NotebookUtils. getLibrary resolves the
# value set that is ACTIVE in this workspace/stage (Dev IDs in Dev, Prod IDs in Prod).
vl = notebookutils.variableLibrary.getLibrary(VL_NAME)
WORKSPACE_ID        = vl.WorkspaceId
BRONZE_LAKEHOUSE_ID = vl.BronzeLakehouseId
SILVER_LAKEHOUSE_ID = vl.SilverLakehouseId
GOLD_LAKEHOUSE_ID   = vl.GoldLakehouseId

print("Resolved stage bindings from Variable Library:")
print(f"  WorkspaceId        = {WORKSPACE_ID}")
print(f"  BronzeLakehouseId  = {BRONZE_LAKEHOUSE_ID}")
print(f"  SilverLakehouseId  = {SILVER_LAKEHOUSE_ID}")
print(f"  GoldLakehouseId    = {GOLD_LAKEHOUSE_ID}")

# Minimal Fabric REST helper (raw REST API, authenticated as the notebook identity).
_FABRIC_BASE = "https://api.fabric.microsoft.com/v1"
def fabric_rest(method, path, body=None):
    token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    resp = requests.request(
        method, f"{_FABRIC_BASE}{path}", headers=headers,
        data=json.dumps(body) if body is not None else None,
    )
    resp.raise_for_status()
    return resp.json() if resp.text else {}

def resolve_item_id(display_name, item_type):
    items = fabric_rest("GET", f"/workspaces/{WORKSPACE_ID}/items?type={item_type}")["value"]
    return next(i["id"] for i in items if i["displayName"] == display_name)

# Cell 5 — (1) Bind each notebook's DEFAULT LAKEHOUSE to this stage's lakehouse
# The default lakehouse lives in the notebook metadata ("dependencies.lakehouse").
# We read each notebook's git-friendly definition, rewrite the three default-lakehouse
# fields to the VL values, and push it back via Update Notebook Definition.
notebook_to_lakehouse = {
    "NB_01_Seed_Bronze":      ("Bronze_LH", BRONZE_LAKEHOUSE_ID),
    "NB_02_Transform_Silver": ("Silver_LH", SILVER_LAKEHOUSE_ID),
    "NB_03_Aggregate_Gold":   ("Gold_LH",   GOLD_LAKEHOUSE_ID),
}

for nb_name, (lh_name, lh_id) in notebook_to_lakehouse.items():
    src = labs.notebook.get_notebook_definition(nb_name, workspace=WORKSPACE_ID, decode=True)
    src = re.sub(r'("default_lakehouse"\s*:\s*")[^"]*(")',              rf'\g<1>{lh_id}\g<2>', src)
    src = re.sub(r'("default_lakehouse_name"\s*:\s*")[^"]*(")',         rf'\g<1>{lh_name}\g<2>', src)
    src = re.sub(r'("default_lakehouse_workspace_id"\s*:\s*")[^"]*(")', rf'\g<1>{WORKSPACE_ID}\g<2>', src)
    labs.notebook.update_notebook_definition(name=nb_name, notebook_content=src, workspace=WORKSPACE_ID)
    print(f"✅ {nb_name}: default lakehouse -> {lh_name} ({lh_id})")

# Cell 6 — (2) Bind Dataflow Gen2 DF_Gold_PA parameters to this stage's Silver lakehouse
# DF Gen2 can't be rebound by a data-source rule; its parameters carry the IDs. We patch
# the parameter DEFAULT literals inside the dataflow's mashup definition via REST.
# Both Silver IDs live in this stage's single workspace, so SilverWorkspaceId = WorkspaceId.
DATAFLOW_NAME = "DF_Gold_PA"
df_id = resolve_item_id(DATAFLOW_NAME, "Dataflow")

param_values = {
    "SilverWorkspaceId": WORKSPACE_ID,
    "SilverLakehouseId": SILVER_LAKEHOUSE_ID,
}

definition = fabric_rest("POST", f"/workspaces/{WORKSPACE_ID}/items/{df_id}/getDefinition")["definition"]
changed = False
for part in definition["parts"]:
    if part["path"].lower().endswith(("mashup.pq", "querymetadata.json")):
        text = base64.b64decode(part["payload"]).decode("utf-8")
        for p_name, p_val in param_values.items():
            # M parameter default:  shared <Param> = "<old>"   and JSON form  "<Param>": "<old>"
            text, n1 = re.subn(rf'(shared\s+{p_name}\s*=\s*")[^"]*(")', rf'\g<1>{p_val}\g<2>', text)
            text, n2 = re.subn(rf'("{p_name}"\s*:\s*")[^"]*(")',        rf'\g<1>{p_val}\g<2>', text)
            if n1 or n2:
                changed = True
        part["payload"] = base64.b64encode(text.encode("utf-8")).decode("utf-8")

if changed:
    fabric_rest(
        "POST", f"/workspaces/{WORKSPACE_ID}/items/{df_id}/updateDefinition",
        {"definition": definition},
    )
    print(f"✅ {DATAFLOW_NAME}: parameters set -> {param_values}")
else:
    print(f"⚠ {DATAFLOW_NAME}: no matching parameter literals found — verify parameter names "
          f"({', '.join(param_values)}) exist in the dataflow.")

# Cell 7 — (3) Rebind Gold_SM (Direct Lake ON ONELAKE) to this stage's Gold_LH
# A data-source deployment rule is NOT supported for Direct Lake on OneLake; instead we
# regenerate the model's connection in code (the "connection-string parameter" the docs
# mention). use_sql_endpoint=False == Direct Lake OVER ONELAKE (not the SQL endpoint).
directlake.update_direct_lake_model_connection(
    dataset="Gold_SM",
    workspace=WORKSPACE_ID,
    source=GOLD_LAKEHOUSE_ID,
    source_type="Lakehouse",
    source_workspace=WORKSPACE_ID,
    use_sql_endpoint=False,
)
print("✅ Gold_SM: Direct Lake on OneLake connection -> Gold_LH (this stage)")

# Sanity check — confirm where the model now points (should be this stage's Gold_LH)
print(directlake.get_direct_lake_sources("Gold_SM", workspace=WORKSPACE_ID))

# Cell 8 — Summary
print("\nStage binding complete — all three bindings now point at this stage's items:")
print(f"  Notebooks NB_01/02/03 -> Bronze/Silver/Gold_LH in workspace {WORKSPACE_ID}")
print(f"  DF_Gold_PA            -> SilverWorkspaceId/SilverLakehouseId = {WORKSPACE_ID} / {SILVER_LAKEHOUSE_ID}")
print(f"  Gold_SM (Direct Lake) -> Gold_LH {GOLD_LAKEHOUSE_ID}")
print("Next pipeline activities (NB_Setup -> NB_01 -> NB_02 -> DF_Gold_PA -> NB_03) now load THIS stage.")
