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
#   It also folds in the old NB_00_Setup_Environment: a pre-flight check verifies the three
#   lakehouses exist in this workspace, so NB_00 is no longer needed.
#
# What it does (all binding values come from VL_CICD_Bindings, never hardcoded):
#   0. Variable Library VL_CICD_Bindings -> created if missing (4 String vars, Dev/Prod sets)
#   0b. Active value set -> switched to match THIS workspace (dev->Development, prod->Production)
#        so getLibrary() returns Dev IDs in Dev and Prod IDs in Prod automatically.
#   0c. Pre-flight -> verifies Bronze_LH / Silver_LH / Gold_LH exist in THIS workspace
#        (folded in from NB_00_Setup_Environment; fails fast if any is missing).
#   1. Notebooks NB_01/02/03 -> ALL three lakehouses attached; DEFAULT = Bronze/Silver/Gold
#        for NB_01/NB_02/NB_03 respectively (this stage).
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
# Library: this notebook needs 'semantic-link-labs'. Cell 2 %pip-installs it for a quick demo,
#   but for production attach a Fabric ENVIRONMENT with the library pre-installed (see Cell 2).
# -----------------------------------------------------------------------------------

# Cell 1 — PARAMETERS (tag this cell as "parameters" in the Fabric notebook).
# Names only — the code resolves the GUIDs at runtime via the Fabric REST API. These are
# used ONLY the FIRST time this notebook runs (to seed the Variable Library's Dev/Prod
# value sets). After the library exists they are ignored. Lakehouse names are identical in
# every stage; only the workspace name differs per stage.
dev_workspace_name  = "ws-CICD-DevTest"
prod_workspace_name = "ws-CICD-PROD"

bronze_lakehouse_name = "Bronze_LH"
silver_lakehouse_name = "Silver_LH"
gold_lakehouse_name   = "Gold_LH"

# Cell 2 — Imports
# NOTE: %pip install runs on EVERY session start (slow cold start) and is per-session only.
#   For production / scheduled pipeline runs, prefer a Fabric ENVIRONMENT instead: create a
#   custom Environment with 'semantic-link-labs' in its Public Libraries, attach it to this
#   notebook (or set it as the workspace default), and DELETE this %pip line. The library is
#   then pre-installed on the Spark pool — faster, reproducible, and version-pinned.
#   See: https://learn.microsoft.com/fabric/data-engineering/environment-manage-library
# %pip install -q semantic-link-labs   # using a Fabric Environment with the library pre-installed
import json, base64, re, requests, io, contextlib, time
import sempy_labs as labs
from sempy_labs import directlake
from sempy_labs import variable_library as vlib
import notebookutils

VL_NAME = "VL_CICD_Bindings"

# Helper to keep output clean: the sempy_labs wrappers print their own verbose 🟢 status
# lines. Wrap those calls in `with _quiet():` to suppress that chatter so this notebook can
# print one tidy summary instead. Exceptions still propagate (only stdout is muted).
@contextlib.contextmanager
def _quiet():
    with contextlib.redirect_stdout(io.StringIO()):
        yield

# Cell 3 — Fabric REST helpers (resolve NAMES -> GUIDs). Control-plane only; no lakehouse.
_FABRIC_BASE = "https://api.fabric.microsoft.com/v1"

def _poll_lro(initial_resp, headers):
    """Poll a Fabric long-running operation (HTTP 202) to completion and return its result.
    getDefinition/updateDefinition are async: the first call returns 202 + a Location header
    pointing at the operation status. We poll until Succeeded, then GET the operation result
    (the actual definition payload). Returns {} for operations that have no result body.

    Tuned for speed: notebook/dataflow definitions are small (source, not data), so these
    operations usually finish almost immediately. Rather than always waiting the server's
    Retry-After (often 2s+), we start with a short 0.4s poll and back off geometrically up
    to a 5s cap. This returns small ops in well under a second while still being polite for
    rare slow ones. If the server sends an explicit Retry-After we honour it as a floor."""
    op_url = initial_resp.headers.get("Location")
    if not op_url:
        return {}
    delay     = 0.4   # first poll fires quickly — most ops are already done
    max_delay = 5.0   # cap so a slow op never busy-waits too tightly
    while True:
        time.sleep(delay)
        poll = requests.get(op_url, headers=headers)
        if not poll.ok:
            raise RuntimeError(
                f"Fabric LRO status poll failed: HTTP {poll.status_code} {poll.reason}.\n"
                f"Response: {poll.text[:800] if poll.text else '(empty)'}"
            )
        state  = poll.json() if poll.text else {}
        status = state.get("status")
        if status == "Succeeded":
            break
        if status == "Failed":
            raise RuntimeError(f"Fabric long-running operation failed: {state}")
        # Still running: back off geometrically, but never below a server-requested Retry-After.
        server_floor = float(poll.headers.get("Retry-After", 0) or 0)
        delay = max(min(delay * 1.6, max_delay), server_floor)
    # Operation done — fetch the result payload (present for getDefinition; absent for updates).
    result_url = poll.headers.get("Location") or (op_url.rstrip("/") + "/result")
    res = requests.get(result_url, headers=headers)
    return res.json() if (res.ok and res.text) else {}

def fabric_rest(method, path, body=None):
    """Call the Fabric REST API and surface a clear error if it fails."""
    token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    resp = requests.request(
        method, f"{_FABRIC_BASE}{path}", headers=headers,
        data=json.dumps(body) if body is not None else None,
    )
    if not resp.ok:
        detail = resp.text[:800] if resp.text else "(empty response body)"
        hint = ""
        if resp.status_code in (401, 403):
            hint = " — the running identity lacks permission on this item/workspace."
        elif resp.status_code == 404:
            hint = " — the workspace or item id does not exist (or is not visible)."
        raise RuntimeError(
            f"Fabric REST {method} {path} failed: HTTP {resp.status_code} {resp.reason}.{hint}\n"
            f"Response: {detail}"
        )
    # 202 Accepted -> long-running operation: poll to completion and return its result.
    if resp.status_code == 202:
        return _poll_lro(resp, headers)
    return resp.json() if resp.text else {}

def resolve_workspace_id(name):
    wss = fabric_rest("GET", "/workspaces")["value"]
    match = next((w["id"] for w in wss if w["displayName"] == name), None)
    if match is None:
        available = ", ".join(sorted(w["displayName"] for w in wss)) or "(none visible)"
        raise ValueError(
            f"Workspace '{name}' not found or not visible to the running identity.\n"
            f"  - Check the name is EXACT and case-sensitive (parameters cell, Cell 1).\n"
            f"  - Check this identity has at least Viewer access to that workspace.\n"
            f"  Workspaces currently visible: {available}"
        )
    return match

def resolve_item_id(workspace_id, display_name, item_type):
    items = fabric_rest("GET", f"/workspaces/{workspace_id}/items?type={item_type}")["value"]
    match = next((i["id"] for i in items if i["displayName"] == display_name), None)
    if match is None:
        available = ", ".join(sorted(i["displayName"] for i in items)) or "(none)"
        raise ValueError(
            f"{item_type} '{display_name}' not found in workspace {workspace_id}.\n"
            f"  - Check the name is EXACT and case-sensitive.\n"
            f"  - Confirm the item was deployed to this stage.\n"
            f"  {item_type}s present: {available}"
        )
    return match

def current_workspace():
    """Return (id, name) of the workspace THIS notebook is running in."""
    ctx = notebookutils.runtime.context
    ws_id = ctx.get("currentWorkspaceId")
    ws_name = ctx.get("currentWorkspaceName")
    if not ws_name and ws_id:
        ws_name = fabric_rest("GET", f"/workspaces/{ws_id}")["displayName"]
    return ws_id, ws_name

# Cell 4 — Create the Variable Library IF it does not already exist (idempotent bootstrap)
# Resolves each stage's GUIDs from the NAMES above. Development is the default value set;
# Production overrides each value. The deployment pipeline activates 'Production' in Prod
# so consumers resolve Prod IDs there. Re-running simply skips creation (no resolution).
# NOTE: to seed BOTH value sets here, the identity creating the library must be able to
# READ the Dev and Prod workspaces. After creation the library is the source of truth.
existing = vlib.list_variable_libraries()
_name_col = next((c for c in existing.columns if "name" in c.lower()), None)
_already_exists = _name_col is not None and VL_NAME in existing[_name_col].tolist()
if not _already_exists:
    def resolve_stage(ws_name):
        ws_id = resolve_workspace_id(ws_name)
        return {
            "WorkspaceId":       ws_id,
            "BronzeLakehouseId": resolve_item_id(ws_id, bronze_lakehouse_name, "Lakehouse"),
            "SilverLakehouseId": resolve_item_id(ws_id, silver_lakehouse_name, "Lakehouse"),
            "GoldLakehouseId":   resolve_item_id(ws_id, gold_lakehouse_name, "Lakehouse"),
        }
    dev  = resolve_stage(dev_workspace_name)
    prod = resolve_stage(prod_workspace_name)

    notes = {
        "WorkspaceId":       "Current-stage workspace; all three lakehouses + DF/SM live here.",
        "BronzeLakehouseId": "Bronze_LH id -> NB_01 default lakehouse.",
        "SilverLakehouseId": "Silver_LH id -> NB_02 default lakehouse + DF_Gold_PA SilverLakehouseId.",
        "GoldLakehouseId":   "Gold_LH id -> NB_03 default lakehouse + Gold_SM Direct Lake source.",
    }
    # NOTE: the create_variable_library wrapper accepts only Boolean/DateTime/Number/
    # Integer/String, so GUIDs are stored as "String" values (Fabric still treats them
    # as plain text identifiers; consumers read the literal GUID string).
    variables  = [{"name": k, "type": "String", "value": dev[k], "note": notes[k]} for k in notes]
    value_sets = [
        {"name": "Development", "variableOverrides": [{"name": k, "value": dev[k]}  for k in notes]},
        {"name": "Production",  "variableOverrides": [{"name": k, "value": prod[k]} for k in notes]},
    ]
    with _quiet():
        vlib.create_variable_library(
            name=VL_NAME,
            variables=variables,
            value_sets=value_sets,
            value_sets_order=["Development", "Production"],
            description="CI/CD stage bindings consumed by notebooks, DF_Gold_PA, and (via this notebook) Gold_SM.",
        )
    print(f"→ Variable Library '{VL_NAME}' created (Development default, Production override).")
else:
    print(f"→ Variable Library '{VL_NAME}' already exists — reused as the source of truth.")

# Cell 5 — Activate the value set that matches THIS workspace (dev->Development, prod->Production)
# THIS is what makes "dev values in Dev, prod values in Prod" happen. getLibrary() (Cell 7)
# returns whichever value set is ACTIVE, so before reading we flip the active set to match the
# workspace this notebook is running in. Running NB_04 as the first pipeline activity per stage
# therefore self-selects the correct IDs — no manual step and no deployment rule required.
# (You may still add a Variable Library deployment rule in the pipeline; this just automates it.)
CURRENT_WS_ID, CURRENT_WS_NAME = current_workspace()
if CURRENT_WS_NAME == prod_workspace_name:
    ACTIVE_SET = "Production"
elif CURRENT_WS_NAME == dev_workspace_name:
    ACTIVE_SET = "Development"
else:
    ACTIVE_SET = None  # unknown stage — leave whatever is active and validate after reading

if ACTIVE_SET:
    vl_id = resolve_item_id(CURRENT_WS_ID, VL_NAME, "VariableLibrary")
    fabric_rest(
        "PATCH", f"/workspaces/{CURRENT_WS_ID}/variableLibraries/{vl_id}",
        {"properties": {"activeValueSetName": ACTIVE_SET}},
    )
    print(f"→ Active value set for '{VL_NAME}' set to '{ACTIVE_SET}' (workspace '{CURRENT_WS_NAME}').")
else:
    print(f"⚠ Workspace '{CURRENT_WS_NAME}' is neither the Dev nor Prod name in Cell 1 — "
          f"leaving the active value set unchanged.")

# Cell 6 — Pre-flight: verify the three lakehouses exist in THIS workspace
# Folds in the former NB_00_Setup_Environment. Lakehouses are created by workspace setup
# (deployment pipeline / manual), not by this notebook — so fail fast with a clear message
# if any is missing before doing any binding work. Names are identical in every stage.
_expected_lakehouses = [bronze_lakehouse_name, silver_lakehouse_name, gold_lakehouse_name]
_present_lakehouses  = {lh.displayName for lh in notebookutils.lakehouse.list(CURRENT_WS_ID)}
_missing_lakehouses  = [n for n in _expected_lakehouses if n not in _present_lakehouses]
if _missing_lakehouses:
    raise ValueError(
        f"Lakehouse(s) missing in workspace '{CURRENT_WS_NAME}': {', '.join(_missing_lakehouses)}.\n"
        f"  - Create them in Fabric before running this notebook.\n"
        f"  Lakehouses present: {', '.join(sorted(_present_lakehouses)) or '(none)'}"
    )
print(f"→ Lakehouses verified in '{CURRENT_WS_NAME}': {', '.join(_expected_lakehouses)}.")

# Cell 7 — Read the now-active Variable Library value set (single source of truth)
# Notebooks are a Variable Library consumer via NotebookUtils. getLibrary resolves the value
# set we just activated, so these are the CURRENT stage's IDs (Dev in Dev, Prod in Prod).
try:
    vl = notebookutils.variableLibrary.getLibrary(VL_NAME)
    WORKSPACE_ID        = vl.WorkspaceId
    BRONZE_LAKEHOUSE_ID = vl.BronzeLakehouseId
    SILVER_LAKEHOUSE_ID = vl.SilverLakehouseId
    GOLD_LAKEHOUSE_ID   = vl.GoldLakehouseId
except Exception as e:
    raise RuntimeError(
        f"Could not read Variable Library '{VL_NAME}' or one of its variables "
        f"(WorkspaceId/BronzeLakehouseId/SilverLakehouseId/GoldLakehouseId).\n"
        f"  - Confirm the library exists in THIS workspace and a value set is active.\n"
        f"  - Confirm all four variable names match exactly.\n"
        f"  Underlying error: {e}"
    ) from e

if any(v in (None, "") for v in (WORKSPACE_ID, BRONZE_LAKEHOUSE_ID, SILVER_LAKEHOUSE_ID, GOLD_LAKEHOUSE_ID)):
    raise ValueError(
        "Variable Library returned an empty value for one of the bindings. "
        "Check the active value set has all four GUIDs populated for this stage."
    )

# Safety net: the WorkspaceId binding must equal the workspace we are running in. If not, the
# wrong value set is active (e.g. Dev IDs while running in Prod) — fail loudly rather than
# silently binding everything to the other stage.
if CURRENT_WS_ID and WORKSPACE_ID != CURRENT_WS_ID:
    raise ValueError(
        f"Active value set mismatch: the library's WorkspaceId ({WORKSPACE_ID}) is not this "
        f"workspace ({CURRENT_WS_ID}). The wrong value set is active for stage '{CURRENT_WS_NAME}'.\n"
        f"  - Expected value set '{ACTIVE_SET or '(stage unknown)'}' to be active.\n"
        f"  - Re-run after confirming the Dev/Prod workspace names in Cell 1 are correct."
    )

# Cell 8 — (1) Attach ALL THREE lakehouses to every notebook; set the stage-appropriate DEFAULT
# The lakehouse binding lives in the notebook metadata ("dependencies.lakehouse"):
#   - default_lakehouse / _name / _workspace_id  -> the ONE default (Bronze for NB_01,
#     Silver for NB_02, Gold for NB_03 — so each writes to its own layer by default), and
#   - known_lakehouses                           -> the FULL list of attached lakehouses
#     (all three), so every notebook can also reach the other two layers by three-part name.
#
# We use the semantic-link-labs wrappers (get_/update_notebook_definition) instead of raw REST.
# They handle base64 + the long-running operation for us, and — crucially — they fetch the
# notebook in its native GIT-friendly SOURCE format (notebook-content.py with '# META' header
# lines), NOT ipynb. Fabric stores NB_01/02/03 as .py, and asking the service to convert
# .py -> .ipynb fails ("PyToIPynbFailure: file suffix .ipynb is not supported"), so we stay in
# source format and patch the '# META' metadata block directly.
#
# We SET the whole lakehouse block rather than regex-replacing existing fields: a freshly
# deployed notebook may have NO lakehouse metadata at all (the deployment pipeline carries no
# default lakehouse), which is exactly what caused the "No default context found, please attach
# a lakehouse before running spark sql queries with partial namespaces" failure. setdefault()
# guarantees the default + all three lakehouses always end up present.
ALL_LAKEHOUSE_IDS = [BRONZE_LAKEHOUSE_ID, SILVER_LAKEHOUSE_ID, GOLD_LAKEHOUSE_ID]

notebook_to_default = {
    "NB_01_Seed_Bronze":      ("Bronze_LH", BRONZE_LAKEHOUSE_ID),
    "NB_02_Transform_Silver": ("Silver_LH", SILVER_LAKEHOUSE_ID),
    "NB_03_Aggregate_Gold":   ("Gold_LH",   GOLD_LAKEHOUSE_ID),
}

def _set_lakehouse_in_source(source, lakehouse_block):
    """Set dependencies.lakehouse inside a notebook's GIT-friendly source definition.
    The notebook-level metadata is a JSON object spread across the contiguous '# META ...'
    lines just under the '# METADATA ********************' header. We strip the prefix, parse
    the JSON, set the lakehouse block, then re-emit the '# META' lines in place."""
    lines = source.splitlines()
    try:
        marker = next(i for i, ln in enumerate(lines)
                      if ln.strip() == "# METADATA ********************")
    except StopIteration as e:
        raise RuntimeError("Notebook source has no '# METADATA' header — cannot bind lakehouse.") from e
    start = marker + 1
    end = start
    while end < len(lines) and lines[end].startswith("# META"):
        end += 1
    meta = json.loads("\n".join(re.sub(r"^# META ?", "", ln) for ln in lines[start:end]))
    meta.setdefault("dependencies", {})["lakehouse"] = lakehouse_block
    new_meta = ["# META " + l for l in json.dumps(meta, indent=2).splitlines()]
    return "\n".join(lines[:start] + new_meta + lines[end:])

for nb_name, (lh_name, lh_id) in notebook_to_default.items():
    lakehouse_block = {
        "default_lakehouse":              lh_id,
        "default_lakehouse_name":         lh_name,
        "default_lakehouse_workspace_id": WORKSPACE_ID,
        "known_lakehouses":               [{"id": x} for x in ALL_LAKEHOUSE_IDS],
    }
    with _quiet():
        source = labs.get_notebook_definition(nb_name, workspace=WORKSPACE_ID)
    source = _set_lakehouse_in_source(source, lakehouse_block)
    with _quiet():
        labs.update_notebook_definition(nb_name, notebook_content=source, workspace=WORKSPACE_ID)
    print(f"→ {nb_name}: default {lh_name}, attached Bronze_LH + Silver_LH + Gold_LH.")

# Cell 9 — (2) Bind Dataflow Gen2 DF_Gold_PA parameters to this stage's Silver lakehouse
# DF Gen2 can't be rebound by a data-source rule; its parameters carry the IDs. We patch
# the parameter DEFAULT literals inside the dataflow's mashup definition via REST.
# Both Silver IDs live in this stage's single workspace, so SilverWorkspaceId = WorkspaceId.
DATAFLOW_NAME = "DF_Gold_PA"
df_id = resolve_item_id(WORKSPACE_ID, DATAFLOW_NAME, "Dataflow")

param_values = {
    "SilverWorkspaceId": WORKSPACE_ID,
    "SilverLakehouseId": SILVER_LAKEHOUSE_ID,
}

definition = fabric_rest("POST", f"/workspaces/{WORKSPACE_ID}/items/{df_id}/getDefinition").get("definition")
if not definition or "parts" not in definition:
    raise RuntimeError(
        f"{DATAFLOW_NAME}: getDefinition did not return a 'definition.parts' payload. "
        "The item may be a long-running op or an unexpected type — verify it is a Dataflow Gen2."
    )
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
    DF_BOUND = True
    print(f"→ {DATAFLOW_NAME}: SilverWorkspaceId/SilverLakehouseId set to this stage's Silver_LH.")
else:
    DF_BOUND = False
    print(f"⚠ {DATAFLOW_NAME}: no matching parameter literals found — verify parameter names "
          f"({', '.join(param_values)}) exist in the dataflow.")

# Cell 10 — (3) Rebind Gold_SM (Direct Lake ON ONELAKE) to this stage's Gold_LH
# A data-source deployment rule is NOT supported for Direct Lake on OneLake; instead we
# regenerate the model's connection in code (the "connection-string parameter" the docs
# mention). use_sql_endpoint=False == Direct Lake OVER ONELAKE (not the SQL endpoint).
with _quiet():
    directlake.update_direct_lake_model_connection(
        dataset="Gold_SM",
        workspace=WORKSPACE_ID,
        source=GOLD_LAKEHOUSE_ID,
        source_type="Lakehouse",
        source_workspace=WORKSPACE_ID,
        use_sql_endpoint=False,
    )
print("→ Gold_SM: Direct Lake on OneLake connection set to this stage's Gold_LH.")

# Cell 11 — Summary (concise, stage-aware)
_line = "─" * 64
print()
print("═" * 64)
print("  NB_04 — stage binding complete")
print("═" * 64)
print(f"  Stage workspace : {CURRENT_WS_NAME}  ({WORKSPACE_ID})")
print(f"  Active value set: {ACTIVE_SET or '(unchanged)'}")
print(_line)
print(f"  {'Notebook':<24}{'Default LH':<12}Attached")
print(f"  {'NB_01_Seed_Bronze':<24}{'Bronze_LH':<12}Bronze + Silver + Gold")
print(f"  {'NB_02_Transform_Silver':<24}{'Silver_LH':<12}Bronze + Silver + Gold")
print(f"  {'NB_03_Aggregate_Gold':<24}{'Gold_LH':<12}Bronze + Silver + Gold")
print(_line)
print(f"  DF_Gold_PA   -> Silver_LH params {'set' if DF_BOUND else 'UNCHANGED (check names)'}")
print("  Gold_SM      -> Gold_LH (Direct Lake on OneLake)")
print("═" * 64)
print("  Next: NB_01 -> NB_02 -> DF_Gold_PA -> NB_03 now load THIS stage.")
