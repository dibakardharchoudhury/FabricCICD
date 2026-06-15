"""
Script: 04_bind_lakehouses_to_notebooks.py
Purpose: Bind the workspace's Bronze_LH / Silver_LH / Gold_LH lakehouses to each
         notebook by injecting the *current* lakehouse GUIDs into the notebook
         metadata via the Fabric REST updateDefinition API.
Run: python 04_bind_lakehouses_to_notebooks.py
Requirements: pip install requests
─────────────────────────────────────────────────────────────────────────────
WHY THIS EXISTS
  Fabric notebooks store the attached lakehouses as *runtime object GUIDs* in
  their metadata. Those GUIDs are unique to each workspace and change whenever a
  workspace/lakehouse is recreated — so they must NOT be committed to Git.
  The repo therefore ships notebooks with an empty `"dependencies": {}` block.

  This script resolves the live lakehouse GUIDs **by display name** in the target
  workspace and writes them back into each notebook, replacing the manual
  "attach all three lakehouses in the UI" step. Run it once per workspace after a
  Git sync / deployment (Dev, Test, Prod). It is idempotent and never hardcodes
  a GUID in source control.
"""

import base64
import json
import re
import time

import requests

# ── Configuration — edit these ───────────────────────────────────────────────
TENANT_ID     = "YOUR_TENANT_ID"
CLIENT_ID     = "YOUR_CLIENT_ID"
CLIENT_SECRET = "YOUR_CLIENT_SECRET"

# Target workspace to bind. Either set WORKSPACE_ID directly, or set
# WORKSPACE_NAME to resolve it by name (e.g. "ws-CICD-DevTest", "PAB-Test").
WORKSPACE_ID   = ""                 # e.g. "292e18c3-b95e-42d1-bb02-9a2064fee5b8"
WORKSPACE_NAME = "ws-CICD-DevTest"  # used only when WORKSPACE_ID is empty

FABRIC_BASE_URL = "https://api.fabric.microsoft.com/v1"
TOKEN_URL       = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

LAKEHOUSE_NAMES = ["Bronze_LH", "Silver_LH", "Gold_LH"]

# Which lakehouse is the *default* for each notebook. All three are always
# attached (known_lakehouses) so cross-lakehouse three-part names resolve;
# the default just sets the pinned context. NB_00 needs no binding.
NOTEBOOK_DEFAULTS = {
    "NB_00_Setup_Environment": None,        # no lakehouse needed (verifies only)
    "NB_01_Seed_Bronze":       "Bronze_LH",
    "NB_02_Transform_Silver":  "Silver_LH",
    "NB_03_Aggregate_Gold":    "Gold_LH",
}


# ── Authentication ────────────────────────────────────────────────────────────
def get_token() -> str:
    resp = requests.post(TOKEN_URL, data={
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope":         "https://api.fabric.microsoft.com/.default",
        "grant_type":    "client_credentials",
    })
    resp.raise_for_status()
    print("✅ Token acquired")
    return resp.json()["access_token"]


def _headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


# ── Resolve workspace + lakehouse GUIDs (by display name) ─────────────────────
def resolve_workspace_id(token: str) -> str:
    if WORKSPACE_ID:
        return WORKSPACE_ID
    resp = requests.get(f"{FABRIC_BASE_URL}/workspaces", headers=_headers(token))
    resp.raise_for_status()
    for ws in resp.json().get("value", []):
        if ws["displayName"] == WORKSPACE_NAME:
            print(f"✅ Workspace '{WORKSPACE_NAME}' → {ws['id']}")
            return ws["id"]
    raise ValueError(f"Workspace '{WORKSPACE_NAME}' not found")


def list_items(token: str, workspace_id: str, item_type: str) -> list:
    """List items of a given type, following continuation tokens."""
    items, url = [], f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/items?type={item_type}"
    while url:
        resp = requests.get(url, headers=_headers(token))
        resp.raise_for_status()
        body = resp.json()
        items.extend(body.get("value", []))
        cont = body.get("continuationToken")
        url = (f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/items?type={item_type}"
               f"&continuationToken={cont}") if cont else None
    return items


def resolve_lakehouse_guids(token: str, workspace_id: str) -> dict:
    lakehouses = {lh["displayName"]: lh["id"]
                  for lh in list_items(token, workspace_id, "Lakehouse")}
    missing = [n for n in LAKEHOUSE_NAMES if n not in lakehouses]
    if missing:
        raise ValueError(
            f"Lakehouse(s) not found in workspace: {', '.join(missing)}. "
            f"Create them (exact names) before binding.")
    print("✅ Resolved lakehouse GUIDs:")
    for name in LAKEHOUSE_NAMES:
        print(f"   {name}: {lakehouses[name]}")
    return {n: lakehouses[n] for n in LAKEHOUSE_NAMES}


# ── Notebook definition get / patch / update ──────────────────────────────────
def get_notebook_definition(token: str, workspace_id: str, notebook_id: str) -> dict:
    resp = requests.post(
        f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/notebooks/{notebook_id}/getDefinition",
        headers=_headers(token),
    )
    resp = _wait_if_async(token, resp)
    resp.raise_for_status()
    return resp.json()["definition"]


def build_dependencies_block(workspace_id: str, lakehouse_guids: dict,
                             default_name: str) -> str:
    """Render the `# META`-prefixed dependencies block for the notebook header."""
    default_id = lakehouse_guids[default_name]
    known = ",\n".join(
        '# META         {\n'
        f'# META           "id": "{lakehouse_guids[name]}"\n'
        '# META         }'
        for name in LAKEHOUSE_NAMES
    )
    return (
        '# META   "dependencies": {\n'
        '# META     "lakehouse": {\n'
        f'# META       "default_lakehouse": "{default_id}",\n'
        f'# META       "default_lakehouse_name": "{default_name}",\n'
        f'# META       "default_lakehouse_workspace_id": "{workspace_id}",\n'
        '# META       "known_lakehouses": [\n'
        f'{known}\n'
        '# META       ]\n'
        '# META     }\n'
        '# META   }'
    )


def patch_notebook_payload(content_b64: str, workspace_id: str,
                           lakehouse_guids: dict, default_name: str) -> str:
    """Replace the `"dependencies": {...}` block in notebook-content.py."""
    src = base64.b64decode(content_b64).decode("utf-8")
    new_block = build_dependencies_block(workspace_id, lakehouse_guids, default_name)

    # Matches both the empty `# META   "dependencies": {}` and a previously
    # populated multi-line dependencies block.
    pattern = re.compile(
        r'# META   "dependencies": \{.*?# META   \}',
        re.DOTALL,
    )
    if not pattern.search(src):
        raise ValueError("Could not locate a dependencies block to patch")
    patched = pattern.sub(new_block.replace("\\", "\\\\"), src, count=1)
    return base64.b64encode(patched.encode("utf-8")).decode("utf-8")


def update_notebook_definition(token: str, workspace_id: str, notebook_id: str,
                               definition: dict, new_content_b64: str) -> None:
    parts = []
    for part in definition["parts"]:
        if part["path"] == "notebook-content.py":
            parts.append({"path": part["path"], "payload": new_content_b64,
                          "payloadType": "InlineBase64"})
        else:
            parts.append(part)

    resp = requests.post(
        f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/notebooks/{notebook_id}/updateDefinition",
        headers=_headers(token),
        json={"definition": {"parts": parts}},
    )
    resp = _wait_if_async(token, resp)
    if resp.status_code not in (200, 202):
        print(f"   ❌ updateDefinition failed: HTTP {resp.status_code}\n   {resp.text}")
        resp.raise_for_status()


def _wait_if_async(token: str, resp: requests.Response) -> requests.Response:
    """Poll long-running operations (202) until they complete."""
    if resp.status_code != 202:
        return resp
    op_url = resp.headers.get("Location")
    retry = int(resp.headers.get("Retry-After", 3))
    for _ in range(40):
        time.sleep(retry)
        poll = requests.get(op_url, headers={"Authorization": f"Bearer {token}"})
        status = poll.json().get("status", "Unknown")
        if status in ("Succeeded", "Completed"):
            # Fetch the result payload if the operation exposes one.
            result = requests.get(op_url.rstrip("/") + "/result",
                                  headers={"Authorization": f"Bearer {token}"})
            return result if result.status_code == 200 else poll
        if status == "Failed":
            raise RuntimeError(f"Operation failed: {poll.text}")
    raise TimeoutError("Long-running operation timed out")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("Fabric CI/CD Demo — Bind Lakehouses to Notebooks")
    print("=" * 60)

    token        = get_token()
    workspace_id = resolve_workspace_id(token)
    guids        = resolve_lakehouse_guids(token, workspace_id)

    notebooks = {nb["displayName"]: nb["id"]
                 for nb in list_items(token, workspace_id, "Notebook")}

    print("\nBinding notebooks:")
    for nb_name, default_lh in NOTEBOOK_DEFAULTS.items():
        if nb_name not in notebooks:
            print(f"  ⚠️  {nb_name}: not found in workspace — skipped")
            continue
        if default_lh is None:
            print(f"  ⏭️  {nb_name}: no lakehouse binding needed — skipped")
            continue

        nb_id      = notebooks[nb_name]
        definition = get_notebook_definition(token, workspace_id, nb_id)
        content    = next(p["payload"] for p in definition["parts"]
                          if p["path"] == "notebook-content.py")
        new_content = patch_notebook_payload(content, workspace_id, guids, default_lh)
        update_notebook_definition(token, workspace_id, nb_id, definition, new_content)
        print(f"  ✅ {nb_name}: default={default_lh}, all three lakehouses attached")

    print("\n✅ Lakehouse binding complete.")
    print("   Restart any open notebook Spark sessions to pick up the new context.")
    print("   NOTE: do NOT commit these GUIDs back to Git — they are workspace-specific.")


if __name__ == "__main__":
    main()
