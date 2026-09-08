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



# Deploys Fabric Git source items and applies stage-specific bindings.

# Cell 1 — Parameters
target_workspace_name = "ws-FabricCICD-PROD"      # the stage to deploy INTO
environment           = "Production"          # "Development" | "Production"

# Use a local checkout when available; Fabric falls back to a Key Vault-authenticated clone.
local_repo_path = ""                           # "" = auto-detect; path = use it; "SKIP_AUTODETECT" = force clone

git_repo_url   = "https://github.com/dibakardharchoudhury/FabricCICD.git"
git_branch     = "main"
key_vault_url  = "https://akvfabcapnew.vault.azure.net/"   # Key Vault holding the GitHub PAT secret
git_pat_secret = "github-pat"                 # KV secret holding a repo-scoped GitHub PAT
repo_subdir    = ""                            # subfolder inside the repo that holds the
                                               # *.Notebook/*.DataPipeline folders ("" = root)

# Build parameter.yml from live Dev IDs.
generate_parameter_yml = True
dev_workspace_name     = "ws-FabricCICD-DEV"     # SOURCE stage; its GUIDs become find_value

# Rebind Direct Lake models after publication.
rebind_direct_lake     = True

# Repository discovery intersects this fabric-cicd allow-list.
_supported_item_types = [
    "Notebook", "Dataflow", "DataPipeline", "SemanticModel", "Report", "Lakehouse",
    "Environment", "VariableLibrary", "Eventhouse", "KQLDatabase", "KQLQueryset",
    "KQLDashboard", "Eventstream", "Warehouse", "MirroredDatabase", "SQLDatabase",
    "Reflex", "CopyJob", "GraphQLApi", "MountedDataFactory", "SparkJobDefinition",
    "DataAgent", "ApacheAirflowJob", "UserDataFunction",
]

include_lakehouses = True

# Never publish the insecure local deployment notebook.
exclude_item_name_regex = r"^NB_04_Deploy_NOTSECURE$"

# Remove items in the target workspace that are no longer in Git (orphans). Leave False
# until you trust the deploy; True keeps the workspace exactly mirroring the repo.
remove_orphans = False

# Cell 2 — Optional session-local dependencies
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
    """Expose notebookutils tokens through azure-core's TokenCredential interface."""
    def get_token(self, *scopes, **kwargs):
        scope = scopes[0] if scopes else "https://api.fabric.microsoft.com/.default"
        audience = scope.split("/.default")[0]
        try:
            tok = notebookutils.credentials.getToken(audience)
        except Exception:
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
    """Find a Fabric source checkout from this file or the current directory."""
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

# Prefer an explicit or detected checkout; clone only inside Fabric.
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
        _pat = notebookutils.credentials.getSecret(key_vault_url, git_pat_secret)
        _clone_dir = tempfile.mkdtemp(prefix="fabric_cicd_")
        _auth_url = git_repo_url.replace("https://", f"https://{_pat}:x-oauth-basic@")
        subprocess.run(
            ["git", "clone", "--depth", "1", "--branch", git_branch, _auth_url, _clone_dir],
            check=True, capture_output=True, text=True,
        )
        repo_directory = os.path.join(_clone_dir, repo_subdir) if repo_subdir else _clone_dir
        _say(f"Cloned <b>{git_branch}</b> → <code>{repo_directory}</code>", "ok")

# Cell 4b — Generate parameter.yml from live Dev IDs
import yaml

# Discover nested Fabric item folders.
def _discover_repo_items(repo_dir):
    found = []
    for root, dirs, _files in os.walk(repo_dir):
        dirs[:] = [d for d in dirs if d != ".git" and not d.startswith(".")]
        item_dirs = []
        for dirname in dirs:
            display_name, separator, item_type = dirname.rpartition(".")
            if separator and item_type in _supported_item_types:
                found.append((item_type, display_name, os.path.join(root, dirname)))
                item_dirs.append(dirname)
        dirs[:] = [d for d in dirs if d not in item_dirs]
    return found

_repo_item_dirs = _discover_repo_items(repo_directory)
item_type_in_scope = sorted({item_type for item_type, _name, _path in _repo_item_dirs})
# Apply deployment toggles and exclude the unused Variable Library.
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

    repo_items = {}
    for _item_type, _display_name, _path in _repo_item_dirs:
        repo_items.setdefault(_item_type, []).append(_display_name)

    def _dev_guid(itype, name):
        """Dev GUID for a repo item resolved BY NAME; None if it isn't in Dev yet (harmless)."""
        try:
            return resolve_item_id(dev_ws_id, name, itype)
        except ValueError:
            return None

    def _repl(token):    # same token for every environment column fabric-cicd may select
        return {k: token for k in dict.fromkeys(["Development", "Production", environment])}

    find_replace = []
# Rewrite workspace IDs outside semantic model definitions.
    _ws_rewrite_types = [t for t in item_type_in_scope if t != "SemanticModel"]
    _rule1 = {"find_value": dev_workspace, "replace_value": _repl("$workspace.$id")}
    if "SemanticModel" in item_type_in_scope and _ws_rewrite_types:
        _rule1["item_type"] = _ws_rewrite_types    # scope to all-but-SemanticModel
    find_replace.append(_rule1)

    # Rewrite pipeline activity item IDs.
    for _itype in ("Notebook", "Dataflow", "SemanticModel"):
        for _nm in sorted(repo_items.get(_itype, [])):
            _gid = _dev_guid(_itype, _nm)
            if _gid:
                find_replace.append({"find_value": _gid,
                                     "replace_value": _repl(f"$items.{_itype}.{_nm}.$id"),
                                     "item_type": ["DataPipeline"]})

    # Rewrite notebook and dataflow lakehouse IDs.
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

# Cell 5 — Publish in-scope items
target = FabricWorkspace(
    workspace_id=target_workspace_id,
    repository_directory=repo_directory,
    item_type_in_scope=item_type_in_scope,
    environment=environment,
    token_credential=credential,
)

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

# Cell 6b — Rebind Direct Lake models to target lakehouses
if rebind_direct_lake:
    import re, glob
    from sempy_labs import directlake

    # Dev lakehouse id -> name, to translate the GUID inside each Direct Lake path to a name.
    _dev_ws_dl = resolve_workspace_id(dev_workspace_name)
    _dev_lh_by_id = {i["id"]: i["displayName"]
                     for i in _fabric_get(f"/workspaces/{_dev_ws_dl}/items?type=Lakehouse")["value"]}

    _rebound = []
    _semantic_model_dirs = sorted(
        (_name, _path)
        for _type, _name, _path in _repo_item_dirs
        if _type == "SemanticModel"
    )
    for _sm_name, _sm_dir in _semantic_model_dirs:
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

# Cell 7 — Summary and temporary clone cleanup
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

# Cell 8 — Guarded standalone target teardown
confirm_delete_workspace = "ws-FabricCICD-PROD"        # set to target_workspace_name (e.g. "ws-FabricCICD-PROD") to actually delete

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

# SQL endpoints are deleted with their parent items.
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
    # Delete dependents first and retry transient dependency failures.
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
