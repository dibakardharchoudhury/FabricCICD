# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "86313016-e213-a285-4d09-9801e4f0072b",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************



# Fabric Notebook: NB_04_Deploy
# Purpose: ONE notebook that DEPLOYS the repo into the current stage's workspace using
#   fabric-cicd, replacing BOTH the slow Fabric Deployment Pipeline AND the post-deploy
#   binding repair a separate notebook used to do. fabric-cicd publishes each item's definition straight
#   from the Git source-format repo via the public Create/Update APIs, and the companion
#   parameter.yml (repo root) rewrites every Dev GUID to the TARGET workspace's GUIDs AS
#   it publishes — so the data pipeline's notebook/dataflow/semantic-model references land
#   already pointed at THIS stage instead of Dev.
#
# What this fixes (your problem): the activities in PL_Refresh_Master reference the
#   notebooks/dataflow by their DEV workspaceId + DEV item GUIDs. After a plain copy they
#   still call the Dev items cross-workspace. parameter.yml swaps:
#     - the Dev workspaceId            -> $workspace.$id            (this stage)
#     - each Dev notebookId            -> $items.Notebook.<name>.$id
#     - the Dev dataflowId             -> $items.Dataflow.DF_Gold_PA.$id
#   The pipeline no longer contains a semantic-model refresh activity (it was removed) —
#   Gold_SM is refreshed inside NB_03's PySpark node right after the Gold tables are written,
#   so there is no datasetId in the pipeline left to re-point. The SemanticModel is still
#   DEPLOYED and Direct-Lake-rebound to this stage's lakehouse (Cell 6b), just not refreshed
#   from the pipeline.
#
# IMPORTANT — repository format:
#   fabric-cicd reads the FABRIC GIT SOURCE FORMAT (item folders like
#   `NB_01_Seed_Bronze.Notebook/` with `.platform` + `notebook-content.py`,
#   `DF_Gold_PA.Dataflow/`, `PL_Refresh_Master.DataPipeline/`, `Gold_SM.SemanticModel/`).
#   That layout is produced when the workspace is Git-connected and you COMMIT FROM Fabric.
#   Point REPO_DIRECTORY at the repo root that contains those item folders + parameter.yml.
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
key_vault_url  = "https://akvFabCap.vault.azure.net/"   # Key Vault holding the GitHub PAT secret
git_pat_secret = "github-pat"                 # KV secret holding a repo-scoped GitHub PAT
repo_subdir    = ""                            # subfolder inside the repo that holds the
                                               # *.Notebook/*.DataPipeline folders ("" = root)

# --- Build parameter.yml from live Dev GUIDs + post-deploy rebind (formerly a separate notebook) -----
# generate_parameter_yml: when True, NB_04 OVERWRITES parameter.yml in the working copy with
#   values resolved AT DEPLOY TIME, so you never hand-maintain GUIDs. EVERY item is DISCOVERED
#   FROM THE REPO (the <DisplayName>.<ItemType> folders) — notebooks, dataflows, semantic models
#   and lakehouses — and each one's Dev GUID is resolved BY NAME from the Dev workspace. Nothing
#   is hardcoded: add an item to the repo and it is picked up automatically. Each find_value is a
#   DEV literal GUID; each replace_value is a fabric-cicd dynamic token that resolves against the
#   TARGET stage. Set False to use the checked-in parameter.yml as-is.
generate_parameter_yml = True
dev_workspace_name     = "ws-CICD-DevTest"     # SOURCE stage; its GUIDs become find_value

# rebind_direct_lake: after publish, re-point every Direct-Lake-on-OneLake semantic model in the
#   repo to THIS stage's matching lakehouse. Each SM is DISCOVERED from the repo; its OneLake
#   source path (.../<workspaceGuid>/<lakehouseGuid>) is read from the model definition, the Dev
#   lakehouse GUID is translated back to a name, and the model is rebound to the SAME-named
#   lakehouse in the target stage. Needs semantic-link-labs + ownership of the model(s). A
#   find_replace cannot safely rewrite a Direct Lake expression, so this is done in code here.
rebind_direct_lake     = True

# NOTE: the Variable Library (VL_CICD_Bindings) is intentionally NOT deployed. Every stage
#   binding is done by parameter.yml's $items tokens (notebook default lakehouses, dataflow
#   Silver, pipeline notebook/dataflow/SM refs) plus the Direct Lake rebind below — nothing in
#   the runtime pipeline reads the VL. It is therefore excluded from publish scope (Cell 4b) and
#   never code-managed.

# Item types fabric-cicd may publish are DISCOVERED FROM THE REPO AT RUN TIME (Cell 4b), NOT
# hardcoded here: Cell 4b scans repo_directory for `*.<ItemType>` folders and intersects them
# with the supported allow-list below, so item_type_in_scope contains EXACTLY the types present
# in the repo. Absent types (Eventhouse, Warehouse, KQL*, etc.) are never in scope — nothing
# extra is published, and with remove_orphans=True nothing extra is deleted. The list never
# needs editing as the repo grows or shrinks.
#
# _supported_item_types is ONLY the allow-list the discovery intersects against (it filters out
# stray non-item folders such as `.git`).
#
# LAKEHOUSE is deployed from the repo like every other item (include_lakehouses=True, default):
#   - The lakehouses live in the repo (Bronze_LH/Silver_LH/Gold_LH). They are authored in Dev and
#     DEPLOYED to every other stage here. fabric-cicd pairs them by the logical id in each
#     .platform, so the SAME repo deployed to each stage keeps the ids lined up.
#   - fabric-cicd is IDEMPOTENT: a lakehouse that already exists in the target is left as-is and
#     only updated when its definition changed — it is never blindly recreated.
#   - Because the lakehouses are in scope, the $items.Lakehouse.<name>.$id tokens in parameter.yml
#     resolve, so the notebook default-lakehouse and dataflow Silver bindings rebind natively
#     (no strip-before-publish, no post-publish code rebind).
#   - A Lakehouse deploy creates the SHELL only (no table data); NB_01/02/03 + DF_Gold still
#     populate the tables at runtime.
_supported_item_types = [
    "Notebook", "Dataflow", "DataPipeline", "SemanticModel", "Report", "Lakehouse",
    "Environment", "VariableLibrary", "Eventhouse", "KQLDatabase", "KQLQueryset",
    "KQLDashboard", "Eventstream", "Warehouse", "MirroredDatabase", "SQLDatabase",
    "Reflex", "CopyJob", "GraphQLApi", "MountedDataFactory", "SparkJobDefinition",
    "DataAgent", "ApacheAirflowJob", "UserDataFunction",
]

# include_lakehouses: True (default) = DEPLOY the repo's lakehouses like every other item and let
# fabric-cicd pair/update them by logical id (idempotent — existing lakehouses are updated only on
# change). This is what makes the $items.Lakehouse.<name>.$id tokens resolve. Set False ONLY if
# the target's lakehouses are managed entirely outside this repo AND already share its logical ids.
include_lakehouses = True

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
#%pip install -q fabric-cicd semantic-link-labs

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

# Cell 4b — Generate parameter.yml on the fly from live Dev GUIDs.
# Instead of hand-maintaining parameter.yml, DISCOVER every item from the repo's
# <DisplayName>.<ItemType> folders and resolve each one's Dev GUID BY NAME from the Dev
# workspace via resolve_item_id. fabric-cicd auto-detects the file we write here.
import yaml

# --- DYNAMIC publish scope -----------------------------------------------------------------
# Build item_type_in_scope FROM THE REPO at run time instead of a hardcoded list. A Fabric
# source-format repo stores every item as a `<DisplayName>.<ItemType>` folder (e.g.
# `NB_01_Seed_Bronze.Notebook`, `Gold_SM.SemanticModel`). We scan repo_directory for those
# folders, take the suffix after the last dot, and keep only suffixes fabric-cicd supports.
# Result: ONLY the item types actually committed in the repo are published — no Eventhouse,
# Warehouse, KQL*, etc. iterations when those folders don't exist. (`Publishing Workspace
# Folders` in fabric-cicd's log is just the folder hierarchy being mirrored, not an item type.)
def _discover_item_types(repo_dir):
    found = set()
    for _n in os.listdir(repo_dir):
        if os.path.isdir(os.path.join(repo_dir, _n)) and "." in _n:
            _suffix = _n.rsplit(".", 1)[1]
            if _suffix in _supported_item_types:
                found.add(_suffix)
    return found

item_type_in_scope = sorted(_discover_item_types(repo_directory))
# Trim the discovered set per the Cell 1 toggles:
#   - drop Lakehouse unless include_lakehouses (avoids duplicate-lakehouse risk),
#   - always drop VariableLibrary: nothing in the runtime pipeline consumes VL_CICD_Bindings
#     (all bindings come from parameter.yml $items tokens), so it is never deployed.
if not include_lakehouses:
    item_type_in_scope = [t for t in item_type_in_scope if t != "Lakehouse"]
item_type_in_scope = [t for t in item_type_in_scope if t != "VariableLibrary"]
_say(f"Discovered <b>{len(item_type_in_scope)}</b> item type(s) in the repo → publishing "
     f"<code>{', '.join(item_type_in_scope) or '(none)'}</code>.", "info")

def resolve_item_id(ws_id, display_name, item_type):
    """Name->GUID resolver (GET-only version)."""
    items = _fabric_get(f"/workspaces/{ws_id}/items?type={item_type}")["value"]
    m = next((i["id"] for i in items if i["displayName"] == display_name), None)
    if m is None:
        avail = ", ".join(sorted(i["displayName"] for i in items)) or "(none)"
        raise ValueError(f"{item_type} '{display_name}' not found in workspace {ws_id}. Present: {avail}")
    return m

if generate_parameter_yml:
    dev_ws_id     = resolve_workspace_id(dev_workspace_name)
    dev_workspace = dev_ws_id

    # DISCOVER every item from the repo's <DisplayName>.<ItemType> folders — nothing is
    # hardcoded. repo_items maps {item_type: [display_name, ...]} for the types we reference.
    repo_items = {}
    for _n in os.listdir(repo_directory):
        if os.path.isdir(os.path.join(repo_directory, _n)) and "." in _n:
            _name, _, _suffix = _n.rpartition(".")
            if _suffix in _supported_item_types:
                repo_items.setdefault(_suffix, []).append(_name)

    def _dev_guid(itype, name):
        """Dev GUID for a repo item resolved BY NAME; None if it isn't in Dev yet (harmless)."""
        try:
            return resolve_item_id(dev_ws_id, name, itype)
        except ValueError:
            return None

    def _repl(token):    # same token for every environment column fabric-cicd may select
        return {k: token for k in dict.fromkeys(["Development", "Production", environment])}

    find_replace = []
    # 1. Dev workspace id -> target workspace id, on every in-scope type EXCEPT the SemanticModel.
    #    This fixes the DataPipeline activity workspaceIds, the Notebook default-lakehouse
    #    workspace id, and the Dataflow SilverWorkspaceId parameter — all of which embed the Dev
    #    workspace GUID and are safe to translate. A Direct-Lake-on-OneLake SemanticModel is
    #    EXCLUDED: its definition embeds BOTH the workspace and lakehouse GUID in one OneLake path;
    #    rewriting only the workspace breaks the import ("Workspace Id should be consistent"), so
    #    it is published self-consistent on Dev's lakehouse and rebound to this stage by Cell 6b.
    _ws_rewrite_types = [t for t in item_type_in_scope if t != "SemanticModel"]
    _rule1 = {"find_value": dev_workspace, "replace_value": _repl("$workspace.$id")}
    if "SemanticModel" in item_type_in_scope and _ws_rewrite_types:
        _rule1["item_type"] = _ws_rewrite_types    # scope to all-but-SemanticModel
    find_replace.append(_rule1)

    # 2. Pipeline activity references: EVERY Notebook / Dataflow in the repo -> its $items token,
    #    scoped to DataPipeline (PL_Refresh_Master calls each by its Dev GUID). SemanticModel is
    #    still scanned for forward-compat (a future pipeline could add a refresh activity); since
    #    the pipeline currently has NO semantic-model refresh, that rule simply finds nothing and
    #    is a harmless no-op. Discovered from the repo, so new pipeline items are picked up with no
    #    code change.
    for _itype in ("Notebook", "Dataflow", "SemanticModel"):
        for _nm in sorted(repo_items.get(_itype, [])):
            _gid = _dev_guid(_itype, _nm)
            if _gid:
                find_replace.append({"find_value": _gid,
                                     "replace_value": _repl(f"$items.{_itype}.{_nm}.$id"),
                                     "item_type": ["DataPipeline"]})

    # 3. EVERY Lakehouse in the repo -> its $items token, for Notebooks AND the Dataflow. This
    #    rebinds notebook default-lakehouse bindings AND DF_Gold_PA's mashup.pq, which references
    #    lakehouses by GUID in two spots (the Silver SOURCE parameter and the inline DESTINATION
    #    query). The Dataflow MUST be included: rule 1 only rewrites the WORKSPACE GUID, so without
    #    these the destination lakehouseId stays the Dev GUID while its workspace is rewritten ->
    #    the refresh fails with EntityNotFound. NOT applied to SemanticModel (Direct Lake, rebound
    #    by Cell 6b) or DataPipeline.
    for _nm in sorted(repo_items.get("Lakehouse", [])):
        _gid = _dev_guid("Lakehouse", _nm)
        if _gid:
            find_replace.append({"find_value": _gid,
                                 "replace_value": _repl(f"$items.Lakehouse.{_nm}.$id"),
                                 "item_type": ["Notebook", "Dataflow"]})

    _hdr = ("# AUTO-GENERATED by NB_04_Deploy from name resolution against the Dev workspace.\n"
            "# Do NOT edit by hand — regenerated on every deploy. find_value = Dev GUID,\n"
            "# replace_value = fabric-cicd dynamic token resolved against the target stage.\n")
    _param_path = os.path.join(repo_directory, "parameter.yml")
    with open(_param_path, "w", encoding="utf-8") as f:
        f.write(_hdr)
        yaml.safe_dump({"find_replace": find_replace}, f, sort_keys=False, default_flow_style=False)
    _say(f"Generated <code>parameter.yml</code> from name resolution "
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

# Cell 6b — Re-point every Direct-Lake-on-OneLake semantic model to THIS stage's matching
# lakehouse. Each SemanticModel is DISCOVERED from the repo; its OneLake source path
# (.../<workspaceGuid>/<lakehouseGuid>) is read from the model definition, the Dev lakehouse
# GUID is translated back to a name, and the model is rebound to the SAME-named lakehouse in the
# target stage. parameter.yml deliberately does NOT rewrite the model's GUIDs (rule 1 excludes
# SemanticModel) because a find_replace cannot safely rebind a Direct Lake expression and
# fabric-cicd has no deployment rule for it — so it is done in code here. use_sql_endpoint=False
# = Direct Lake OVER ONELAKE (not the SQL endpoint). Needs semantic-link-labs + model ownership.
if rebind_direct_lake:
    import re, glob
    from sempy_labs import directlake

    # Dev lakehouse id -> name, to translate the GUID inside each Direct Lake path to a name.
    _dev_ws_dl = resolve_workspace_id(dev_workspace_name)
    _dev_lh_by_id = {i["id"]: i["displayName"]
                     for i in _fabric_get(f"/workspaces/{_dev_ws_dl}/items?type=Lakehouse")["value"]}

    _rebound = []
    for _sm_dir in sorted(glob.glob(os.path.join(repo_directory, "*.SemanticModel"))):
        _sm_name = os.path.basename(_sm_dir).rsplit(".", 1)[0]
        _text = ""
        for _f in glob.glob(os.path.join(_sm_dir, "**", "*.tmdl"), recursive=True):
            with open(_f, encoding="utf-8") as _fh:
                _text += _fh.read() + "\n"
        # Direct Lake on OneLake source path: .../<workspaceGuid>/<lakehouseGuid>
        _m = re.search(r"onelake\.dfs\.fabric\.microsoft\.com/[0-9a-fA-F-]{36}/([0-9a-fA-F-]{36})", _text)
        if not _m:
            continue                                   # not Direct Lake on OneLake -> nothing to do
        _lh_name = _dev_lh_by_id.get(_m.group(1))
        if not _lh_name:
            _say(f"<b>{_sm_name}</b>: Direct Lake lakehouse <code>{_m.group(1)}</code> not found "
                 f"by name in Dev — skipping rebind.", "warn")
            continue
        directlake.update_direct_lake_model_connection(
            dataset=_sm_name, workspace=target_workspace_id,
            source=resolve_item_id(target_workspace_id, _lh_name, "Lakehouse"),
            source_type="Lakehouse", source_workspace=target_workspace_id,
            use_sql_endpoint=False,
        )
        _rebound.append(f"{_sm_name} → {_lh_name}")
    if _rebound:
        _say("Direct Lake re-pointed to this stage: <b>" + "</b>, <b>".join(_rebound) + "</b>.", "ok")
    else:
        _say("No Direct-Lake-on-OneLake semantic models found in the repo to rebind.", "info")
else:
    _say("Direct Lake rebind skipped (rebind_direct_lake=False).", "info")

# Cell 7 — Summary + cleanup of the temp clone.
# Only remove the clone we created — NEVER delete a local/auto-detected working tree.
if _clone_dir:
    shutil.rmtree(_clone_dir, ignore_errors=True)
# Precompute summary cells as plain strings — Fabric's Python (<3.12) forbids a backslash
# inside an f-string expression, so no apostrophes/escapes may appear in the {...} parts below.
_param_summary = "generated from name resolution" if generate_parameter_yml else "checked-in file used as-is"
_dl_summary    = "models re-pointed to this stage's lakehouse" if rebind_direct_lake else "left as published (rebind off)"
_items_summary = ", ".join(item_type_in_scope)
display(HTML(
    '<div style="font-family:Segoe UI,system-ui,sans-serif;border:1px solid #d0d7de;'
    'border-radius:8px;padding:14px 18px;max-width:680px">'
    '<div style="font-weight:700;font-size:15px;margin-bottom:8px">fabric-cicd deploy complete</div>'
    f'<table style="border-collapse:collapse;font-size:13px">'
    f'<tr><td style="padding:2px 14px 2px 0;color:#57606a">Target workspace</td><td><b>{target_workspace_name}</b></td></tr>'
    f'<tr><td style="padding:2px 14px 2px 0;color:#57606a">Environment</td><td><b>{environment}</b></td></tr>'
    f'<tr><td style="padding:2px 14px 2px 0;color:#57606a">Items in scope</td><td>{_items_summary}</td></tr>'
    f'<tr><td style="padding:2px 14px 2px 0;color:#57606a">Pipeline refs</td><td>notebooks / dataflows re-pointed to this stage</td></tr>'
    f'<tr><td style="padding:2px 14px 2px 0;color:#57606a">parameter.yml</td><td>{_param_summary}</td></tr>'
    f'<tr><td style="padding:2px 14px 2px 0;color:#57606a">Direct Lake</td><td>{_dl_summary}</td></tr>'
    f'<tr><td style="padding:2px 14px 2px 0;color:#57606a">Lakehouses</td><td>deployed from repo (paired by logical id; updated only on change)</td></tr>'
    '</table>'
    '<div style="margin-top:10px;font-size:12px;color:#57606a">The data pipeline has no '
    'semantic-model refresh activity — Gold_SM is refreshed inside NB_03_Aggregate_Gold after its '
    'tables are written. Connections (gateway / Direct Lake) are tenant-level and are NOT '
    'auto-mapped — set them once per stage.</div>'
    '</div>'
))



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 8 — DANGER: DELETE EVERY ITEM in the target (PROD) workspace.
# -----------------------------------------------------------------------------------
# Standalone teardown helper — NOT part of the deploy flow above. Run it ON ITS OWN when
# you want a clean slate in the target workspace (e.g. to re-test a from-empty deploy).
# It DYNAMICALLY lists whatever items actually exist in `target_workspace_name` and deletes
# ALL of them (reports, semantic models, pipelines, dataflows, notebooks, lakehouses,
# environments, ...) — no hardcoded allow-list. The workspace itself stays.
#
# It does NOT delete SQL endpoints: a Lakehouse/Warehouse owns a SQLEndpoint child that Fabric
# creates and removes AUTOMATICALLY with its parent, so those are skipped (deleting one directly
# only errors). There is no special "staging lakehouse" handling — every Lakehouse found is just
# deleted like any other item.
#
# This is IRREVERSIBLE — lakehouse tables/data go with the lakehouse. To prevent an
# accidental wipe on "Run all", it is GUARDED: nothing is deleted until you set
# confirm_delete_workspace to EXACTLY the target workspace name. Left as "" it only reports.
#
# SELF-CONTAINED: this cell is meant to be run ON ITS OWN, so it does NOT assume Cells 1–4
# ran in this session. The bootstrap below defines everything it needs (_FABRIC_BASE,
# _fabric_get, _say, target_workspace_name, target_workspace_id) only if they are missing —
# which is why running just this cell no longer raises NameError: '_fabric_get' is not defined.
confirm_delete_workspace = "ws-CICD-PROD"        # set to target_workspace_name (e.g. "ws-CICD-PROD") to actually delete

# Which workspace to wipe. Re-declared here so the cell stands alone; edit if you ran nothing else.
target_workspace_name = globals().get("target_workspace_name", "ws-CICD-PROD")

import requests, notebookutils
from IPython.display import display, HTML

_FABRIC_BASE = globals().get("_FABRIC_BASE", "https://api.fabric.microsoft.com/v1")

# Define _say only if Cell 3 did not (plain-text fallback that still renders an HTML banner).
if "_say" not in globals():
    def _say(msg, kind="ok"):
        _color = {"ok": "#1a7f37", "info": "#0969da", "warn": "#9a6700", "err": "#cf222e"}.get(kind, "#1a7f37")
        _icon  = {"ok": "✓", "info": "ℹ", "warn": "⚠", "err": "✗"}.get(kind, "✓")
        display(HTML(
            f'<div style="font-family:Segoe UI,system-ui,sans-serif;font-size:13px;'
            f'padding:5px 12px;margin:3px 0;border-left:3px solid {_color};'
            f'background:{_color}14;color:#24292f;border-radius:4px">'
            f'<span style="color:{_color};font-weight:700">{_icon}</span>&nbsp; {msg}</div>'
        ))

# Define _fabric_get only if Cell 4 did not.
if "_fabric_get" not in globals():
    def _fabric_get(path):
        token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
        r = requests.get(f"{_FABRIC_BASE}{path}", headers={"Authorization": f"Bearer {token}"})
        r.raise_for_status()
        return r.json()

def _fabric_delete(path):
    token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
    r = requests.delete(f"{_FABRIC_BASE}{path}", headers={"Authorization": f"Bearer {token}"})
    r.raise_for_status()
    return r

# Resolve target_workspace_id by name if Cell 4 did not already (standalone run).
target_workspace_id = globals().get("target_workspace_id")
if not target_workspace_id:
    _wss = _fabric_get("/workspaces")["value"]
    target_workspace_id = next((w["id"] for w in _wss if w["displayName"] == target_workspace_name), None)
    if not target_workspace_id:
        _avail = ", ".join(sorted(w["displayName"] for w in _wss)) or "(none visible)"
        raise ValueError(f"Workspace '{target_workspace_name}' not visible to this identity. Visible: {_avail}")

# List every item currently in the target workspace (fully DYNAMIC — whatever is actually there is
# what gets cleaned up, regardless of type). Then drop SYSTEM-MANAGED CHILD items that must NOT be
# deleted on their own: a Lakehouse (or Warehouse) owns a SQLEndpoint that Fabric creates and DELETES
# AUTOMATICALLY when the parent is removed. Calling DELETE on a SQLEndpoint directly just errors and
# is pointless, so we skip it and let it disappear with its lakehouse. Everything else found in the
# workspace IS deleted, no hardcoded allow-list.
_AUTO_MANAGED_TYPES = {"SQLEndpoint"}   # auto-created AND auto-deleted with their parent Lakehouse/Warehouse
_all_raw    = _fabric_get(f"/workspaces/{target_workspace_id}/items")["value"]
_skipped    = [it for it in _all_raw if it["type"] in _AUTO_MANAGED_TYPES]
_all_items  = [it for it in _all_raw if it["type"] not in _AUTO_MANAGED_TYPES]

if not _all_items:
    _say(f"Target workspace <b>{target_workspace_name}</b> already has no deletable items — nothing to delete.", "info")
elif confirm_delete_workspace != target_workspace_name:
    _say(f"<b>{len(_all_items)}</b> item(s) in <b>{target_workspace_name}</b> would be deleted. "
         f"This is a DRY RUN — set <code>confirm_delete_workspace = \"{target_workspace_name}\"</code> "
         f"and re-run THIS cell to actually delete them.", "warn")
    for _it in _all_items:
        _say(f"&nbsp;&nbsp;• {_it['displayName']} <code>({_it['type']})</code>", "info")
    if _skipped:
        _say(f"Skipping <b>{len(_skipped)}</b> system-managed item(s) "
             f"(auto-deleted with their parent): "
             + ", ".join(f"{_it['displayName']} ({_it['type']})" for _it in _skipped), "info")
else:
    # Delete EVERY remaining item dynamically. _order is ONLY a best-effort ordering hint so that
    # dependents go before their sources (fewer dependency-lock failures); any type NOT listed
    # (now or in future) still gets deleted — it is just ranked last. Three retry passes mop up
    # items whose first delete failed because a dependent had not gone yet.
    _order = ["Report", "DataPipeline", "Dataflow", "Notebook", "SemanticModel",
              "Environment", "Eventstream", "KQLDashboard", "KQLQueryset", "KQLDatabase",
              "Eventhouse", "Warehouse", "SQLDatabase", "MirroredDatabase", "Lakehouse"]
    _rank = {t: i for i, t in enumerate(_order)}
    _pending = sorted(_all_items, key=lambda it: _rank.get(it["type"], len(_order)))
    _deleted, _failed = [], []
    for _pass in range(3):
        if not _pending:
            break
        _still = []
        for _it in _pending:
            try:
                _fabric_delete(f"/workspaces/{target_workspace_id}/items/{_it['id']}")
                _deleted.append(_it)
            except Exception as _e:
                _it["_err"] = str(_e)
                _still.append(_it)
        _pending = _still
    _failed = _pending

    _say(f"Deleted <b>{len(_deleted)}</b> item(s) from <b>{target_workspace_name}</b>.",
         "ok" if not _failed else "warn")
    for _it in _failed:
        _say(f"Could NOT delete <b>{_it['displayName']}</b> <code>({_it['type']})</code>: "
             f"{_it.get('_err', 'unknown error')}", "err")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
