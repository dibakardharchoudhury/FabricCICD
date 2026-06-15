# Fabric Notebook: NB_05_Deploy
# Purpose: ONE notebook that DEPLOYS the repo into the current stage's workspace using
#   fabric-cicd, replacing BOTH the slow Fabric Deployment Pipeline AND the post-deploy
#   binding repair that NB_04 did. fabric-cicd publishes each item's definition straight
#   from the Git source-format repo via the public Create/Update APIs, and the companion
#   parameter.yml (repo root) rewrites every Dev GUID to the TARGET workspace's GUIDs AS
#   it publishes — so the data pipeline's notebook/dataflow/semantic-model references land
#   already pointed at THIS stage instead of Dev.
#
# What this fixes (your problem): the activities in PL_Refresh_Master reference the
#   notebooks/dataflow/semantic model by their DEV workspaceId + DEV item GUIDs. After a
#   plain copy they still call the Dev items cross-workspace. parameter.yml swaps:
#     - the Dev workspaceId            -> $workspace.$id            (this stage)
#     - each Dev notebookId            -> $items.Notebook.<name>.$id
#     - the Dev dataflowId             -> $items.Dataflow.DF_Gold_PA.$id
#     - the SM-refresh datasetId       -> $items.SemanticModel.Gold_SM.$id
#   The semantic-model refresh STAYS as an activity in the pipeline — fabric-cicd just
#   makes it point at this stage's Gold_SM, it does not run the refresh itself.
#
# IMPORTANT — repository format:
#   fabric-cicd reads the FABRIC GIT SOURCE FORMAT (item folders like
#   `NB_01_Seed_Bronze.Notebook/` with `.platform` + `notebook-content.py`,
#   `DF_Gold_PA.Dataflow/`, `PL_Refresh_Master.DataPipeline/`, `Gold_SM.SemanticModel/`).
#   That layout is produced when the workspace is Git-connected and you COMMIT FROM Fabric.
#   Point REPO_DIRECTORY at the root that contains those folders + parameter.yml, NOT at
#   the flat demo files in `notebooks/*.py`.
#
# Run it: standalone (not inside PL_Refresh_Master) as the deploy step — e.g. from a
#   notebook scheduled after a merge to main, or invoked by your GitHub Actions runner.
#   The identity running it must be Member/Admin on the TARGET workspace and own Gold_SM.
# Library: needs 'fabric-cicd'. Cell 2 %pip-installs it for convenience; for production
#   attach a Fabric ENVIRONMENT with fabric-cicd pre-installed and delete the %pip line.
# -----------------------------------------------------------------------------------

# Cell 1 — PARAMETERS (tag this cell as "parameters" in the Fabric notebook).
# environment selects which column of parameter.yml's replace_value is applied. It must
# match the keys used there ("Development" / "Production").
target_workspace_name = "ws-CICD-PROD"      # the stage to deploy INTO
environment           = "Production"          # "Development" | "Production"

# Git source-format repo (the one Fabric commits to). Cloned locally so fabric-cicd can
# read the item folders. Use a Key Vault-backed PAT — never hardcode a token here.
#
# local_repo_path: SHORT-CIRCUIT for runs where the repo is ALREADY on disk (VS Code /
#   Jupyter on your machine, or a GitHub Actions runner after actions/checkout). When set —
#   or auto-detected by walking up from this file for a `.git` folder — Cell 4 uses those
#   files DIRECTLY and SKIPS the git clone + the PAT entirely. Leave "" to auto-detect; the
#   clone is only used as a fallback for the in-Fabric run (the Spark node has no working
#   tree). Set to a literal path to force a specific checkout, or "SKIP_AUTODETECT" to force
#   the clone even when a local tree exists.
local_repo_path = ""                           # "" = auto-detect; path = use it; "SKIP_AUTODETECT" = force clone

# IN-FABRIC RUN: the Spark node has NO working tree, so auto-detect finds nothing and the
# CLONE path below IS used — git_repo_url + key_vault_url + git_pat_secret are all REQUIRED.
git_repo_url   = "https://github.com/dibakardharchoudhury/FabricCICD.git"
git_branch     = "main"
key_vault_url  = "https://<your-kv>.vault.azure.net/"   # <-- set: your Key Vault URL
git_pat_secret = "github-pat"                 # KV secret holding a repo-scoped GitHub PAT
repo_subdir    = ""                            # subfolder inside the repo that holds the
                                               # *.Notebook/*.DataPipeline folders ("" = root)

# --- Merged from NB_04: build parameter.yml from live Dev GUIDs + post-deploy rebind -----
# generate_parameter_yml: when True, NB_05 OVERWRITES parameter.yml in the working copy with
#   values resolved AT DEPLOY TIME, so you never hand-maintain GUIDs. Sources, in order:
#   NB_04's Variable Library 'VL_CICD_Bindings' (workspace + 3 lakehouse Dev GUIDs) and
#   name-resolution for the notebook/dataflow/semantic-model Dev GUIDs the VL does not store.
#   Each find_value is a DEV literal; each replace_value is a fabric-cicd dynamic token that
#   resolves against the TARGET stage. Set False to use the checked-in parameter.yml as-is.
generate_parameter_yml = True
dev_workspace_name     = "ws-CICD-DevTest"     # SOURCE stage; its GUIDs become find_value
vl_name                = "VL_CICD_Bindings"    # NB_04's Variable Library
gen_notebook_names     = ["NB_01_Seed_Bronze", "NB_02_Transform_Silver", "NB_03_Aggregate_Gold"]
gen_dataflow_name      = "DF_Gold_PA"
gen_semantic_model     = "Gold_SM"

# rebind_direct_lake: after publish, re-point Gold_SM's Direct Lake on OneLake connection to
#   THIS stage's Gold_LH (folds in NB_04 Cell 10). Needs semantic-link-labs + ownership of
#   Gold_SM. Replaces the parameter.yml 'semantic_model_binding' block, which would need a
#   tenant connection GUID that is not name-resolvable.
rebind_direct_lake     = True

# manage_variable_library: (FOLDS IN NB_04 Cell 4 + Cell 5) own the Variable Library in CODE
#   instead of letting fabric-cicd publish it from the repo. When True, Cell 6c:
#     - CREATES VL_CICD_Bindings in the TARGET workspace if missing, seeding BOTH the
#       Development and Production value sets from live lakehouse GUIDs resolved by name in
#       dev_workspace_name / prod_workspace_name (exactly like NB_04 Cell 4), and
#       UPDATES the value-set overrides if it already exists so the GUIDs stay current.
#     - ACTIVATES the value set that matches `environment` (Production in Prod), so consumers
#       resolve THIS stage's IDs (NB_04 Cell 5).
#   VariableLibrary is also REMOVED from fabric-cicd's scope below (item_type_in_scope) so the
#   two don't fight — code-managed VL avoids fabric-cicd leaving Development active and avoids
#   the global workspace-id find_replace rewriting the GUIDs stored inside the library.
#   Set False to instead let fabric-cicd publish the VL verbatim from the repo (then YOU must
#   flip the active value set per stage yourself).
manage_variable_library = True
prod_workspace_name     = "ws-CICD-PROD"     # the Production stage; seeds the Production value set

# Item types fabric-cicd is allowed to publish AND (when remove_orphans=True) to delete.
# Default = ALL supported types, so any item present in the repo is picked up automatically
# (e.g. Gold_Dashboard.Report, future Eventhouse/Warehouse/etc.) without editing this list.
# Types with no folder in the repo are simply skipped (harmless no-op).
#
# LAKEHOUSE is toggled separately via include_lakehouses (below) because it needs care:
#   - fabric-cicd pairs items by the logical id in each .platform (NOT by display name). For
#     pairing to work, the SAME logical id must exist on both sides. That holds when every
#     stage's lakehouse originates from this Git repo (Git-connect the target ONCE so the
#     .platform logical ids line up, OR let fabric-cicd create them here on first deploy).
#   - DUPLICATE RISK: if Bronze_LH/Silver_LH/Gold_LH were created independently by workspace
#     setup (their own logical ids, different from the repo's), publishing the repo lakehouses
#     makes a SECOND "Bronze_LH" in the target — then $items.Lakehouse.Bronze_LH.$id in
#     parameter.yml is AMBIGUOUS. To avoid this, either (a) deploy lakehouses from this repo
#     in every stage, or (b) ensure the pre-created lakehouses share the repo's logical ids
#     via the Git connection.
#   - A Lakehouse deploy creates the SHELL only (no table data); NB_01/02/03 + DF_Gold still
#     populate the tables at runtime.
_base_item_types = [
    "Notebook", "Dataflow", "DataPipeline", "SemanticModel", "Report",
    "Environment", "VariableLibrary", "Eventhouse", "KQLDatabase", "KQLQueryset",
    "KQLDashboard", "Eventstream", "Warehouse", "MirroredDatabase", "SQLDatabase",
    "Reflex", "CopyJob", "GraphQLApi", "MountedDataFactory", "SparkJobDefinition",
    "DataAgent", "ApacheAirflowJob", "UserDataFunction",
]

# include_lakehouses: set True to let fabric-cicd publish/manage Lakehouse items from the
# repo; False to leave lakehouses to workspace setup and only reference the existing ones by
# name via $items.Lakehouse.<name>.$id in parameter.yml (avoids the duplicate risk above).
include_lakehouses = False

item_type_in_scope = _base_item_types + (["Lakehouse"] if include_lakehouses else [])
# When the Variable Library is code-managed (Cell 6c), take it OUT of fabric-cicd's scope so
# the two don't double-manage it (see manage_variable_library above).
if manage_variable_library:
    item_type_in_scope = [t for t in item_type_in_scope if t != "VariableLibrary"]

# exclude_item_name_regex: skip items by DISPLAY NAME at publish time (regex). Escape hatch
# only — default "" publishes EVERYTHING, including the semanticlink Environment, exactly as it
# is in the repo. fabric-cicd writes the Environment's Spark compute VERBATIM, so the source
# file (semanticlink.Environment/Setting/Sparkcompute.yml) must already fit the target stage's
# pool. If a future Environment is ever authored against a bigger pool than a stage has and you
# get SparkSettingsComputeExceedsPoolLimit, set this to e.g. "^semanticlink$" to skip just that
# item; the proper fix is to lower its compute in the repo and commit from Fabric.
exclude_item_name_regex = ""   # "" = publish every item; regex = skip matching display names

# Remove items in the target workspace that are no longer in Git (orphans). Leave False
# until you trust the deploy; True keeps the workspace exactly mirroring the repo.
remove_orphans = False

# Cell 2 — Install fabric-cicd (MUST run before Cell 3's import, or you get
#   "ModuleNotFoundError: No module named 'fabric_cicd'"). %pip is a per-session magic:
#   it installs into THIS Spark session only and must be the FIRST code run after a fresh
#   session start — run this cell before any import cell. fabric-cicd needs Python 3.10+
#   (Fabric Runtime 1.2 / 1.3); on the older 3.10 runtime it installs fine.
#   For production, prefer a Fabric ENVIRONMENT with fabric-cicd pre-installed and DELETE
#   this line (faster cold start, version-pinned): https://learn.microsoft.com/fabric/data-engineering/environment-manage-library
# semantic-link-labs is added for the merged Direct Lake rebind (Cell 6b); drop it if you set
# rebind_direct_lake = False and a Fabric Environment already provides fabric-cicd.
%pip install -q fabric-cicd semantic-link-labs

# Cell 3 — Imports, token credential, and a small HTML status helper.
import json, base64, time, subprocess, tempfile, os, shutil
import notebookutils
from IPython.display import display, HTML
from azure.core.credentials import AccessToken
from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

def _say(msg, kind="ok"):
    color = {"ok": "#1a7f37", "info": "#0969da", "warn": "#9a6700", "err": "#cf222e"}.get(kind, "#1a7f37")
    icon  = {"ok": "✓", "info": "ℹ", "warn": "⚠", "err": "✗"}.get(kind, "✓")
    display(HTML(
        f'<div style="font-family:Segoe UI,system-ui,sans-serif;font-size:13px;'
        f'padding:5px 12px;margin:3px 0;border-left:3px solid {color};'
        f'background:{color}14;color:#24292f;border-radius:4px">'
        f'<span style="color:{color};font-weight:700">{icon}</span>&nbsp; {msg}</div>'
    ))

def _jwt_exp(tok):
    """Read the 'exp' epoch-seconds claim from a JWT so AccessToken reports a real expiry."""
    payload = tok.split(".")[1]
    payload += "=" * (-len(payload) % 4)        # pad to a multiple of 4 for base64
    return int(json.loads(base64.urlsafe_b64decode(payload)).get("exp", time.time() + 3600))

class NotebookUtilsCredential:
    """azure-core TokenCredential backed by notebookutils, so fabric-cicd authenticates as
    the identity running this notebook (no service principal needed for an in-Fabric run).
    fabric-cicd asks for the Fabric API scope (and OneLake/storage for some item types);
    notebookutils.credentials.getToken resolves the audience from the scope's resource."""
    def get_token(self, *scopes, **kwargs):
        scope = scopes[0] if scopes else "https://api.fabric.microsoft.com/.default"
        audience = scope.split("/.default")[0]
        try:
            tok = notebookutils.credentials.getToken(audience)
        except Exception:
            # Fall back to the Power BI / Fabric audience alias if the resource URL is rejected.
            tok = notebookutils.credentials.getToken("pbi")
        return AccessToken(tok, _jwt_exp(tok))

credential = NotebookUtilsCredential()

# Cell 4 — Resolve the target workspace id and clone the Git source-format repo locally.
_FABRIC_BASE = "https://api.fabric.microsoft.com/v1"

def _fabric_get(path):
    token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
    import requests
    r = requests.get(f"{_FABRIC_BASE}{path}", headers={"Authorization": f"Bearer {token}"})
    r.raise_for_status()
    return r.json()

def resolve_workspace_id(name):
    wss = _fabric_get("/workspaces")["value"]
    match = next((w["id"] for w in wss if w["displayName"] == name), None)
    if match is None:
        avail = ", ".join(sorted(w["displayName"] for w in wss)) or "(none visible)"
        raise ValueError(f"Workspace '{name}' not visible to this identity. Visible: {avail}")
    return match

target_workspace_id = resolve_workspace_id(target_workspace_name)
_say(f"Target workspace <b>{target_workspace_name}</b> = <code>{target_workspace_id}</code>", "info")

def _looks_like_source_repo(path):
    """A Fabric source-format repo root has parameter.yml and/or *.<ItemType> folders."""
    if not path or not os.path.isdir(path):
        return False
    if os.path.isfile(os.path.join(path, "parameter.yml")):
        return True
    return any("." in n and os.path.isdir(os.path.join(path, n)) for n in os.listdir(path))

def _detect_local_repo():
    """Find an already-checked-out working tree by walking up from this file / cwd for a
    `.git` dir. Returns the repo root (optionally + repo_subdir) or None. Runs only OUTSIDE
    Fabric — the Spark node has no working tree, so this returns None there."""
    starts = []
    try:
        starts.append(os.path.dirname(os.path.abspath(__file__)))   # script / VS Code run
    except NameError:
        pass                                                         # no __file__ in some kernels
    starts.append(os.getcwd())
    for start in starts:
        cur = start
        while True:
            if os.path.isdir(os.path.join(cur, ".git")):
                root = os.path.join(cur, repo_subdir) if repo_subdir else cur
                return root if _looks_like_source_repo(root) else None
            parent = os.path.dirname(cur)
            if parent == cur:
                break
            cur = parent
    return None

# Decide the source of the repo files: explicit local path > auto-detected working tree >
# git clone (fallback for the in-Fabric run). Clone path needs the KV PAT; the others don't.
_clone_dir = None
if local_repo_path and local_repo_path != "SKIP_AUTODETECT":
    repo_directory = os.path.join(local_repo_path, repo_subdir) if repo_subdir else local_repo_path
    if not _looks_like_source_repo(repo_directory):
        raise ValueError(f"local_repo_path '{repo_directory}' is not a Fabric source-format "
                         f"repo (no parameter.yml or *.<ItemType> folders).")
    _say(f"Using local repo (no clone) → <code>{repo_directory}</code>", "ok")
else:
    _auto = None if local_repo_path == "SKIP_AUTODETECT" else _detect_local_repo()
    if _auto:
        repo_directory = _auto
        _say(f"Auto-detected local working tree (no clone) → <code>{repo_directory}</code>", "ok")
    else:
        # Fallback: clone the repo to an ephemeral local dir (e.g. the in-Fabric Spark node,
        # which has no working tree). PAT comes from Key Vault.
        _pat = notebookutils.credentials.getSecret(key_vault_url, git_pat_secret)
        _clone_dir = tempfile.mkdtemp(prefix="fabric_cicd_")
        _auth_url = git_repo_url.replace("https://", f"https://{_pat}:x-oauth-basic@")
        subprocess.run(
            ["git", "clone", "--depth", "1", "--branch", git_branch, _auth_url, _clone_dir],
            check=True, capture_output=True, text=True,
        )
        repo_directory = os.path.join(_clone_dir, repo_subdir) if repo_subdir else _clone_dir
        _say(f"Cloned <b>{git_branch}</b> → <code>{repo_directory}</code>", "ok")

# Cell 4b — (MERGED FROM NB_04) Generate parameter.yml on the fly from live Dev GUIDs.
# Instead of hand-maintaining parameter.yml, build it here from the SAME source of truth
# NB_04 uses: the Variable Library 'VL_CICD_Bindings' (workspace + 3 lakehouse Dev GUIDs)
# plus name-resolution (NB_04's resolve_item_id) for the notebook/dataflow/semantic-model
# Dev GUIDs the VL does not carry. fabric-cicd auto-detects the file we write here.
import yaml

def resolve_item_id(ws_id, display_name, item_type):
    """NB_04's name->GUID resolver (GET-only version)."""
    items = _fabric_get(f"/workspaces/{ws_id}/items?type={item_type}")["value"]
    m = next((i["id"] for i in items if i["displayName"] == display_name), None)
    if m is None:
        avail = ", ".join(sorted(i["displayName"] for i in items)) or "(none)"
        raise ValueError(f"{item_type} '{display_name}' not found in workspace {ws_id}. Present: {avail}")
    return m

def _read_vl_dev_values(ws_id, vl_name_):
    """Read NB_04's Variable Library Development value set from its item definition.
    Development is the default set, so variables.json holds the Dev values; a
    valueSets/Development.json override wins if present. Returns {VarName: value}; {} on any
    issue (caller then falls back to name resolution for everything)."""
    try:
        import requests
        vl_id = resolve_item_id(ws_id, vl_name_, "VariableLibrary")
        token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        r = requests.post(f"{_FABRIC_BASE}/workspaces/{ws_id}/items/{vl_id}/getDefinition", headers=headers)
        if r.status_code == 202:                                  # long-running op -> poll
            op = r.headers.get("Location")
            for _ in range(30):
                time.sleep(0.5)
                p = requests.get(op, headers=headers)
                st = (p.json() if p.text else {}).get("status")
                if st == "Succeeded":
                    r = requests.get(p.headers.get("Location") or op.rstrip("/") + "/result", headers=headers)
                    break
                if st == "Failed":
                    return {}
        parts = (r.json() if r.text else {}).get("definition", {}).get("parts", [])
        decoded = {p["path"]: base64.b64decode(p["payload"]).decode("utf-8") for p in parts}
        vals = {}
        for path, text in decoded.items():                        # defaults (Development set)
            if path.lower().endswith("variables.json"):
                for v in json.loads(text).get("variables", []):
                    if v.get("value"):
                        vals.setdefault(v["name"], v["value"])
        for path, text in decoded.items():                        # explicit Development overrides
            if "development" in path.lower() and path.lower().endswith(".json") and "valueset" in path.lower():
                ov = json.loads(text)
                for o in ov.get("variableOverrides", ov.get("overrides", [])):
                    if o.get("value"):
                        vals[o["name"]] = o["value"]
        return vals
    except Exception as e:
        _say(f"Variable Library '{vl_name_}' not read ({e}); resolving all Dev GUIDs by name.", "warn")
        return {}

if generate_parameter_yml:
    dev_ws_id = resolve_workspace_id(dev_workspace_name)
    vl_vals   = _read_vl_dev_values(dev_ws_id, vl_name)

    def _dev_lh(var_key, lh_name):
        """Prefer the VL's stored Dev lakehouse GUID; fall back to resolving by name."""
        return vl_vals.get(var_key) or resolve_item_id(dev_ws_id, lh_name, "Lakehouse")

    dev_workspace = vl_vals.get("WorkspaceId") or dev_ws_id
    dev_bronze_lh = _dev_lh("BronzeLakehouseId", "Bronze_LH")
    dev_silver_lh = _dev_lh("SilverLakehouseId", "Silver_LH")
    dev_gold_lh   = _dev_lh("GoldLakehouseId",   "Gold_LH")

    # Items the VL does NOT carry -> resolve by name from the Dev workspace (NB_04 concept).
    dev_nb_ids = {n: resolve_item_id(dev_ws_id, n, "Notebook") for n in gen_notebook_names}
    dev_df_id  = resolve_item_id(dev_ws_id, gen_dataflow_name, "Dataflow")
    dev_sm_id  = resolve_item_id(dev_ws_id, gen_semantic_model, "SemanticModel")

    def _repl(token):    # same token for every environment column fabric-cicd may select
        return {k: token for k in dict.fromkeys(["Development", "Production", environment])}

    find_replace = []
    # 1. Dev workspace id -> target workspace id (global; fixes every activity workspaceId).
    find_replace.append({"find_value": dev_workspace, "replace_value": _repl("$workspace.$id")})
    # 2. Pipeline notebook references.
    for n, gid in dev_nb_ids.items():
        find_replace.append({"find_value": gid,
                             "replace_value": _repl(f"$items.Notebook.{n}.$id"),
                             "item_type": ["DataPipeline"]})
    # 3. Pipeline dataflow reference.
    find_replace.append({"find_value": dev_df_id,
                         "replace_value": _repl(f"$items.Dataflow.{gen_dataflow_name}.$id"),
                         "item_type": ["DataPipeline"]})
    # 4. Pipeline SM-refresh datasetId.
    find_replace.append({"find_value": dev_sm_id,
                         "replace_value": _repl(f"$items.SemanticModel.{gen_semantic_model}.$id"),
                         "item_type": ["DataPipeline"]})
    # 5. DF_Gold_PA mashup SilverLakehouseId.
    find_replace.append({"find_value": dev_silver_lh,
                         "replace_value": _repl("$items.Lakehouse.Silver_LH.$id"),
                         "item_type": ["Dataflow"]})
    # 6. Notebook default-lakehouse bindings.
    for lh_name, gid in (("Bronze_LH", dev_bronze_lh), ("Silver_LH", dev_silver_lh), ("Gold_LH", dev_gold_lh)):
        find_replace.append({"find_value": gid,
                             "replace_value": _repl(f"$items.Lakehouse.{lh_name}.$id"),
                             "item_type": ["Notebook"]})

    _hdr = ("# AUTO-GENERATED by NB_05_Deploy from VL_CICD_Bindings + name resolution.\n"
            "# Do NOT edit by hand — regenerated on every deploy. find_value = Dev GUID,\n"
            "# replace_value = fabric-cicd dynamic token resolved against the target stage.\n")
    _param_path = os.path.join(repo_directory, "parameter.yml")
    with open(_param_path, "w", encoding="utf-8") as f:
        f.write(_hdr)
        yaml.safe_dump({"find_replace": find_replace}, f, sort_keys=False, default_flow_style=False)
    _say(f"Generated <code>parameter.yml</code> from <b>{vl_name}</b> + name resolution "
         f"({len(find_replace)} rules; Dev workspace <b>{dev_workspace_name}</b>).", "ok")
else:
    _say("parameter.yml generation skipped (generate_parameter_yml=False) — using the "
         "checked-in file.", "info")

# Cell 5 — Build the FabricWorkspace and publish every in-scope item.
# parameter.yml at repo_directory root is auto-detected; its replace_value column is chosen
# by `environment`. Each item is created/updated in target_workspace_id with TARGET GUIDs.
target = FabricWorkspace(
    workspace_id=target_workspace_id,
    repository_directory=repo_directory,
    item_type_in_scope=item_type_in_scope,
    environment=environment,
    token_credential=credential,
)

# item_name_exclude_regex skips items by display name (see exclude_item_name_regex above) —
# e.g. an Environment whose Spark compute exceeds the target pool. None = publish everything.
publish_all_items(target, item_name_exclude_regex=(exclude_item_name_regex or None))
_say(f"Published all in-scope items into <b>{target_workspace_name}</b> "
     f"(env=<b>{environment}</b>)"
     + (f"; skipped names matching <code>{exclude_item_name_regex}</code>" if exclude_item_name_regex else "")
     + ".", "ok")

# Cell 6 — Optional: remove items in the target workspace that no longer exist in Git.
if remove_orphans:
    unpublish_all_orphan_items(target)
    _say("Removed orphaned items not present in Git.", "warn")
else:
    _say("Orphan cleanup skipped (remove_orphans=False).", "info")

# Cell 6b — (MERGED FROM NB_04 Cell 10) Re-point Gold_SM Direct Lake on OneLake to THIS
# stage's Gold_LH. fabric-cicd publishes the model with Dev's connection baked in; a
# data-source deployment rule is NOT supported for Direct Lake on OneLake, so we regenerate
# the connection in code against the target workspace. use_sql_endpoint=False = Direct Lake
# OVER ONELAKE (not the SQL endpoint). Requires semantic-link-labs and ownership of Gold_SM.
if rebind_direct_lake:
    from sempy_labs import directlake
    _target_gold_lh = resolve_item_id(target_workspace_id, "Gold_LH", "Lakehouse")
    directlake.update_direct_lake_model_connection(
        dataset=gen_semantic_model,
        workspace=target_workspace_id,
        source=_target_gold_lh,
        source_type="Lakehouse",
        source_workspace=target_workspace_id,
        use_sql_endpoint=False,
    )
    _say(f"<b>{gen_semantic_model}</b>: Direct Lake on OneLake re-pointed to this stage's Gold_LH.", "ok")
else:
    _say("Direct Lake rebind skipped (rebind_direct_lake=False).", "info")

# Cell 6c — (MERGED FROM NB_04 Cells 4 + 5) Create/seed VL_CICD_Bindings in the TARGET and
# activate the value set for this stage. fabric-cicd does NOT manage this when
# manage_variable_library=True (we removed VariableLibrary from item_type_in_scope), so the
# library is owned here in code — same source of truth NB_04 used. Create-if-missing seeds
# BOTH value sets from live GUIDs (Development from dev_workspace_name, Production from
# prod_workspace_name); if it already exists we refresh the overrides. Then we PATCH the
# active value set to match `environment` so consumers resolve THIS stage's IDs.
if manage_variable_library:
    from sempy_labs import variable_library as vlib

    def _fabric_patch(path, body):
        import requests
        token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        r = requests.patch(f"{_FABRIC_BASE}{path}", headers=headers, data=json.dumps(body))
        r.raise_for_status()
        return r.json() if r.text else {}

    def _resolve_stage_guids(ws_name):
        """Resolve a stage's workspace + 3 lakehouse GUIDs by name (NB_04 Cell 4 'resolve_stage')."""
        ws_id = resolve_workspace_id(ws_name)
        return {
            "WorkspaceId":       ws_id,
            "BronzeLakehouseId": resolve_item_id(ws_id, "Bronze_LH", "Lakehouse"),
            "SilverLakehouseId": resolve_item_id(ws_id, "Silver_LH", "Lakehouse"),
            "GoldLakehouseId":   resolve_item_id(ws_id, "Gold_LH",   "Lakehouse"),
        }

    _vl_notes = {
        "WorkspaceId":       "Current-stage workspace; all three lakehouses + DF/SM live here.",
        "BronzeLakehouseId": "Bronze_LH id -> NB_01 default lakehouse.",
        "SilverLakehouseId": "Silver_LH id -> NB_02 default lakehouse + DF_Gold_PA SilverLakehouseId.",
        "GoldLakehouseId":   "Gold_LH id -> NB_03 default lakehouse + Gold_SM Direct Lake source.",
    }
    _dev_guids  = _resolve_stage_guids(dev_workspace_name)
    _prod_guids = _resolve_stage_guids(prod_workspace_name)
    # Activate the value set AS-AND-WHEN-REQUIRED, the way NB_04 Cell 5 does: pick the set from
    # WHICH STAGE we are deploying into (target workspace name), not from a free-text param.
    # dev_workspace_name -> Development, prod_workspace_name -> Production; if the target is
    # neither, fall back to the `environment` parameter so unusual stage names still resolve.
    if target_workspace_name == prod_workspace_name:
        _active_set = "Production"
    elif target_workspace_name == dev_workspace_name:
        _active_set = "Development"
    else:
        _active_set = "Production" if environment == "Production" else "Development"

    _existing = vlib.list_variable_libraries(workspace=target_workspace_id)
    _name_col = next((c for c in _existing.columns if "name" in c.lower()), None)
    _vl_present = _name_col is not None and vl_name in _existing[_name_col].tolist()

    if not _vl_present:
        # CREATE with both value sets seeded from live GUIDs (NB_04 Cell 4). GUIDs stored as
        # String values (the wrapper only accepts Boolean/DateTime/Number/Integer/String).
        _variables  = [{"name": k, "type": "String", "value": _dev_guids[k], "note": _vl_notes[k]} for k in _vl_notes]
        _value_sets = [
            {"name": "Development", "variableOverrides": [{"name": k, "value": _dev_guids[k]}  for k in _vl_notes]},
            {"name": "Production",  "variableOverrides": [{"name": k, "value": _prod_guids[k]} for k in _vl_notes]},
        ]
        vlib.create_variable_library(
            name=vl_name,
            variables=_variables,
            value_sets=_value_sets,
            value_sets_order=["Development", "Production"],
            description="CI/CD stage bindings consumed by notebooks, DF_Gold_PA, and Gold_SM.",
            workspace=target_workspace_id,
        )
        _say(f"Variable Library <b>{vl_name}</b> created in <b>{target_workspace_name}</b> "
             f"(Development + Production value sets seeded from live GUIDs).", "ok")
    else:
        # EXISTS -> refresh both value sets so the stored GUIDs stay current (idempotent update).
        try:
            for _set_name, _g in (("Development", _dev_guids), ("Production", _prod_guids)):
                for _k in _vl_notes:
                    vlib.update_variable_library_value_set_value(
                        name=vl_name, value_set_name=_set_name,
                        variable_name=_k, value=_g[_k], workspace=target_workspace_id,
                    )
            _say(f"Variable Library <b>{vl_name}</b> already in <b>{target_workspace_name}</b> "
                 f"— refreshed Development + Production GUIDs.", "info")
        except Exception as _e:
            _say(f"Variable Library <b>{vl_name}</b> exists; value refresh skipped ({_e}).", "warn")

    # ACTIVATE the value set for this stage (NB_04 Cell 5).
    _vl_id = resolve_item_id(target_workspace_id, vl_name, "VariableLibrary")
    _fabric_patch(f"/workspaces/{target_workspace_id}/variableLibraries/{_vl_id}",
                  {"properties": {"activeValueSetName": _active_set}})
    _say(f"Active value set for <b>{vl_name}</b> set to <b>{_active_set}</b> "
         f"(workspace <b>{target_workspace_name}</b>).", "ok")
else:
    _say("Variable Library left to fabric-cicd (manage_variable_library=False) — set the "
         "active value set per stage yourself.", "info")

# Cell 7 — Summary + cleanup of the temp clone.
# Only remove the clone we created — NEVER delete a local/auto-detected working tree.
if _clone_dir:
    shutil.rmtree(_clone_dir, ignore_errors=True)
display(HTML(
    '<div style="font-family:Segoe UI,system-ui,sans-serif;border:1px solid #d0d7de;'
    'border-radius:8px;padding:14px 18px;max-width:680px">'
    '<div style="font-weight:700;font-size:15px;margin-bottom:8px">fabric-cicd deploy complete</div>'
    f'<table style="border-collapse:collapse;font-size:13px">'
    f'<tr><td style="padding:2px 14px 2px 0;color:#57606a">Target workspace</td><td><b>{target_workspace_name}</b></td></tr>'
    f'<tr><td style="padding:2px 14px 2px 0;color:#57606a">Environment</td><td><b>{environment}</b></td></tr>'
    f'<tr><td style="padding:2px 14px 2px 0;color:#57606a">Items in scope</td><td>{", ".join(item_type_in_scope)}</td></tr>'
    f'<tr><td style="padding:2px 14px 2px 0;color:#57606a">Pipeline refs</td><td>notebooks / dataflow / Gold_SM re-pointed to this stage</td></tr>'
    f'<tr><td style="padding:2px 14px 2px 0;color:#57606a">parameter.yml</td><td>{"generated from " + vl_name + " + name resolution" if generate_parameter_yml else "checked-in file used as-is"}</td></tr>'
    f'<tr><td style="padding:2px 14px 2px 0;color:#57606a">Direct Lake</td><td>{"Gold_SM re-pointed to this stage Gold_LH" if rebind_direct_lake else "left as published (rebind off)"}</td></tr>'
    '</table>'
    '<div style="margin-top:10px;font-size:12px;color:#57606a">The data pipeline\'s semantic-model '
    'refresh activity now targets this stage\'s Gold_SM. Connections (gateway / Direct Lake) are '
    'tenant-level and are NOT auto-mapped — set them once per stage.</div>'
    '</div>'
))
