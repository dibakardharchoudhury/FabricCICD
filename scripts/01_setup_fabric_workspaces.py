"""
Script: 01_setup_fabric_workspaces.py
Purpose: Create PAB-Dev, PAB-Test, PAB-Prod workspaces and lakehouses via Fabric REST API
Run: python 01_setup_fabric_workspaces.py
Requirements: pip install requests azure-identity
─────────────────────────────────────────────────────────────────────────────
NOTE: The workspace must be on a Fabric capacity for lakehouse creation to work.
"""

import requests
import json
import time

# ── Configuration — edit these ───────────────────────────────────────────────
TENANT_ID        = "YOUR_TENANT_ID"
CLIENT_ID        = "YOUR_CLIENT_ID"
CLIENT_SECRET    = "YOUR_CLIENT_SECRET"
CAPACITY_ID      = "YOUR_FABRIC_CAPACITY_ID"  # F2+ capacity GUID

FABRIC_BASE_URL  = "https://api.fabric.microsoft.com/v1"
TOKEN_URL        = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

WORKSPACES = [
    {"name": "PAB-Dev",  "description": "PAB CI/CD Demo — Development stage"},
    {"name": "PAB-Test", "description": "PAB CI/CD Demo — Test stage"},
    {"name": "PAB-Prod", "description": "PAB CI/CD Demo — Production stage"},
]

LAKEHOUSES = ["Bronze_LH", "Silver_LH", "Gold_LH"]

# ── Authentication ────────────────────────────────────────────────────────────
def get_token() -> str:
    resp = requests.post(TOKEN_URL, data={
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope":         "https://api.fabric.microsoft.com/.default",
        "grant_type":    "client_credentials",
    })
    resp.raise_for_status()
    token = resp.json()["access_token"]
    print("✅ Token acquired")
    return token

# ── Create Workspace ─────────────────────────────────────────────────────────
def create_workspace(token: str, name: str, description: str) -> dict:
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {
        "displayName": name,
        "description": description,
        "capacityId":  CAPACITY_ID,
    }
    resp = requests.post(f"{FABRIC_BASE_URL}/workspaces", headers=headers, json=payload)

    if resp.status_code == 409:
        print(f"  ⚠️  Workspace '{name}' already exists — fetching existing ID")
        # Find existing workspace
        list_resp = requests.get(f"{FABRIC_BASE_URL}/workspaces", headers=headers)
        list_resp.raise_for_status()
        workspaces = list_resp.json().get("value", [])
        for ws in workspaces:
            if ws["displayName"] == name:
                print(f"  → Found: {ws['id']}")
                return ws
        raise ValueError(f"Workspace '{name}' not found after 409")

    resp.raise_for_status()
    workspace = resp.json()
    print(f"  ✅ Created workspace '{name}' — ID: {workspace['id']}")
    return workspace

# ── Create Lakehouse ─────────────────────────────────────────────────────────
def create_lakehouse(token: str, workspace_id: str, lakehouse_name: str) -> dict:
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {
        "displayName": lakehouse_name,
        "type":        "Lakehouse",
    }
    resp = requests.post(
        f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/items",
        headers=headers,
        json=payload
    )

    if resp.status_code == 202:
        # Long-running operation — poll for completion
        operation_url = resp.headers.get("Location") or resp.headers.get("x-ms-operation-id")
        print(f"    ⏳ Creating lakehouse (async)… polling operation")
        for _ in range(20):
            time.sleep(5)
            if operation_url and operation_url.startswith("http"):
                poll_resp = requests.get(operation_url, headers=headers)
                status = poll_resp.json().get("status", "Unknown")
            else:
                status = "Unknown"
            print(f"    Status: {status}")
            if status in ("Succeeded", "succeeded"):
                print(f"    ✅ Lakehouse '{lakehouse_name}' created")
                return poll_resp.json()
            if status in ("Failed", "failed"):
                raise RuntimeError(f"Lakehouse creation failed: {poll_resp.json()}")
        raise TimeoutError("Lakehouse creation timed out")

    if resp.status_code in (200, 201):
        item = resp.json()
        print(f"    ✅ Lakehouse '{lakehouse_name}' — ID: {item['id']}")
        return item

    if resp.status_code == 409:
        print(f"    ⚠️  Lakehouse '{lakehouse_name}' already exists")
        return {}

    resp.raise_for_status()
    return {}

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("Fabric CI/CD Demo — Workspace & Lakehouse Setup")
    print("=" * 60)

    token = get_token()
    workspace_ids = {}

    for ws_config in WORKSPACES:
        print(f"\n📁 Creating workspace: {ws_config['name']}")
        ws = create_workspace(token, ws_config["name"], ws_config["description"])
        workspace_ids[ws_config["name"]] = ws["id"]

        print(f"  Creating lakehouses in {ws_config['name']}:")
        for lh_name in LAKEHOUSES:
            create_lakehouse(token, ws["id"], lh_name)
            time.sleep(2)  # Brief pause between items

    print("\n" + "=" * 60)
    print("✅ Setup Complete — Workspace IDs:")
    for name, wid in workspace_ids.items():
        print(f"  {name}: {wid}")

    # Save IDs for use in other scripts
    with open("workspace_ids.json", "w") as f:
        json.dump(workspace_ids, f, indent=2)
    print("\nSaved to workspace_ids.json — use in subsequent scripts")

if __name__ == "__main__":
    main()
