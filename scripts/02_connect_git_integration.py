"""
Script: 02_connect_git_integration.py
Purpose: Connect PAB-Dev workspace to GitHub via Fabric REST API
Run: python 02_connect_git_integration.py
Requirements: pip install requests
─────────────────────────────────────────────────────────────────────────────
NOTE: Git integration can also be configured via Workspace Settings UI.
This script automates it for repeatability.
"""

import requests
import json

# ── Configuration ─────────────────────────────────────────────────────────────
TENANT_ID        = "YOUR_TENANT_ID"
CLIENT_ID        = "YOUR_CLIENT_ID"
CLIENT_SECRET    = "YOUR_CLIENT_SECRET"

GITHUB_PAT        = "YOUR_GITHUB_PAT"               # Classic or fine-grained PAT
GITHUB_REPO_URL   = "https://github.com/ORG/fabric-pab-cicd"
GITHUB_BRANCH     = "develop"
GITHUB_FOLDER     = "/fabric"                        # Subfolder in repo

FABRIC_BASE_URL   = "https://api.fabric.microsoft.com/v1"

# Load workspace IDs from previous script
with open("workspace_ids.json") as f:
    WORKSPACE_IDS = json.load(f)

DEV_WORKSPACE_ID  = WORKSPACE_IDS["PAB-Dev"]

# ── Authentication ────────────────────────────────────────────────────────────
def get_token() -> str:
    resp = requests.post(
        f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token",
        data={
            "client_id":     CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope":         "https://api.fabric.microsoft.com/.default",
            "grant_type":    "client_credentials",
        }
    )
    resp.raise_for_status()
    return resp.json()["access_token"]

# ── Connect Workspace to Git ──────────────────────────────────────────────────
def connect_git(token: str, workspace_id: str) -> None:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json"
    }

    # Step 1: Add GitHub account credentials
    print("Step 1: Adding GitHub credentials to workspace...")
    cred_payload = {
        "gitCredentials": {
            "gitCredentialsType": "GitHub",
            "gitHubAccessToken": GITHUB_PAT
        }
    }
    resp = requests.post(
        f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/git/connect",
        headers=headers,
        json=cred_payload
    )

    # Step 2: Set up Git connection (provider, repo, branch, folder)
    print("Step 2: Configuring Git repository connection...")
    git_payload = {
        "gitProviderDetails": {
            "gitProviderType": "GitHub",
            "repositoryUrl":   GITHUB_REPO_URL,
            "branchName":      GITHUB_BRANCH,
            "directoryName":   GITHUB_FOLDER,
        },
        "initializationStrategy": "PreferWorkspace"
        # Use "PreferRemote" to initialize workspace from Git branch content
        # Use "PreferWorkspace" to push workspace content to Git (recommended for first setup)
    }

    resp = requests.post(
        f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/git/initializeConnection",
        headers=headers,
        json=git_payload
    )

    if resp.status_code in (200, 202):
        print(f"✅ Git integration configured:")
        print(f"   Repository:  {GITHUB_REPO_URL}")
        print(f"   Branch:      {GITHUB_BRANCH}")
        print(f"   Folder:      {GITHUB_FOLDER}")
        print(f"   Strategy:    PreferWorkspace (workspace content pushed to Git)")
    else:
        print(f"❌ Failed: HTTP {resp.status_code}")
        print(resp.json())
        resp.raise_for_status()

# ── Check Git Status ──────────────────────────────────────────────────────────
def get_git_status(token: str, workspace_id: str) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(
        f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/git/status",
        headers=headers
    )
    resp.raise_for_status()
    return resp.json()

# ── Commit All Changes ────────────────────────────────────────────────────────
def commit_all(token: str, workspace_id: str, message: str) -> None:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json"
    }

    # Get status to find all changed items
    status = get_git_status(token, workspace_id)
    changed_items = [
        item["itemId"]
        for item in status.get("changes", [])
        if item.get("conflictType") != "Conflict"
    ]

    if not changed_items:
        print("✅ No uncommitted changes — workspace is synced with Git")
        return

    print(f"Committing {len(changed_items)} items...")

    commit_payload = {
        "comment": message,
        "items": [{"objectId": item_id} for item_id in changed_items]
    }

    resp = requests.post(
        f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/git/commitToGit",
        headers=headers,
        json=commit_payload
    )

    if resp.status_code in (200, 202):
        print(f"✅ Committed {len(changed_items)} items: {message}")
    else:
        print(f"❌ Commit failed: HTTP {resp.status_code}")
        print(resp.json())

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("Fabric Git Integration Setup")
    print("=" * 60)

    token = get_token()
    print(f"✅ Token acquired")

    print(f"\nConnecting PAB-Dev ({DEV_WORKSPACE_ID}) to GitHub...")
    connect_git(token, DEV_WORKSPACE_ID)

    print("\nChecking Git status...")
    status = get_git_status(token, DEV_WORKSPACE_ID)
    changes = status.get("changes", [])
    print(f"Items with changes: {len(changes)}")
    for item in changes[:5]:
        print(f"  - {item.get('itemDisplayName', 'Unknown')} [{item.get('remoteChange', item.get('localChange', '?'))}]")

    print("\nCommitting initial state to Git...")
    commit_all(
        token,
        DEV_WORKSPACE_ID,
        "feat: initial Medallion architecture — Bronze/Silver/Gold layers"
    )

    print("\n✅ Git integration setup complete!")

if __name__ == "__main__":
    main()
