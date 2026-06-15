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
git_repo_url   = "https://github.com/<org>/<repo>.git"
git_branch     = "main"
key_vault_url  = "https://<your-kv>.vault.azure.net/"
git_pat_secret = "github-pat"                 # KV secret holding a repo-scoped PAT
repo_subdir    = ""                            # subfolder inside the repo that holds the
                                               # *.Notebook/*.DataPipeline folders ("" = root)

# Only items of these types are published. Lakehouses are created by workspace setup and
# resolved by $items.Lakehouse.<name>.$id against the EXISTING ones, so they are NOT in scope.
item_type_in_scope = ["Notebook", "Dataflow", "DataPipeline", "SemanticModel"]

# Remove items in the target workspace that are no longer in Git (orphans). Leave False
# until you trust the deploy; True keeps the workspace exactly mirroring the repo.
remove_orphans = False

# Cell 2 — Install fabric-cicd (prefer a Fabric Environment in production; then delete this).
# %pip install -q fabric-cicd

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

# Clone the repo to an ephemeral local dir (driver node). PAT comes from Key Vault.
_pat = notebookutils.credentials.getSecret(key_vault_url, git_pat_secret)
_clone_dir = tempfile.mkdtemp(prefix="fabric_cicd_")
_auth_url = git_repo_url.replace("https://", f"https://{_pat}:x-oauth-basic@")
subprocess.run(
    ["git", "clone", "--depth", "1", "--branch", git_branch, _auth_url, _clone_dir],
    check=True, capture_output=True, text=True,
)
repo_directory = os.path.join(_clone_dir, repo_subdir) if repo_subdir else _clone_dir
_say(f"Cloned <b>{git_branch}</b> → <code>{repo_directory}</code>", "ok")

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

publish_all_items(target)
_say(f"Published all in-scope items into <b>{target_workspace_name}</b> "
     f"(env=<b>{environment}</b>).", "ok")

# Cell 6 — Optional: remove items in the target workspace that no longer exist in Git.
if remove_orphans:
    unpublish_all_orphan_items(target)
    _say("Removed orphaned items not present in Git.", "warn")
else:
    _say("Orphan cleanup skipped (remove_orphans=False).", "info")

# Cell 7 — Summary + cleanup of the temp clone.
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
    '</table>'
    '<div style="margin-top:10px;font-size:12px;color:#57606a">The data pipeline\'s semantic-model '
    'refresh activity now targets this stage\'s Gold_SM. Connections (gateway / Direct Lake) are '
    'tenant-level and are NOT auto-mapped — set them once per stage.</div>'
    '</div>'
))
