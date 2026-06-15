"""
Script: 03_create_deployment_pipeline.py
Purpose: Create Fabric Deployment Pipeline (Dev → Test → Prod) via REST API
Run: python 03_create_deployment_pipeline.py
Requirements: pip install requests
"""

import requests
import json
import time

# ── Configuration ─────────────────────────────────────────────────────────────
TENANT_ID        = "YOUR_TENANT_ID"
CLIENT_ID        = "YOUR_CLIENT_ID"
CLIENT_SECRET    = "YOUR_CLIENT_SECRET"

FABRIC_BASE_URL  = "https://api.fabric.microsoft.com/v1"

# Load workspace IDs from script 01
with open("workspace_ids.json") as f:
    WORKSPACE_IDS = json.load(f)

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

# ── Create Deployment Pipeline ────────────────────────────────────────────────
def create_pipeline(token: str) -> str:
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    payload = {
        "displayName": "PAB_DeployPipeline",
        "description": "CI/CD deployment pipeline for PAB Medallion architecture — Dev→Test→Prod",
        "stages": [
            {"order": 0, "displayName": "Development", "description": "Active development"},
            {"order": 1, "displayName": "Test",        "description": "QA and validation"},
            {"order": 2, "displayName": "Production",  "description": "Live environment"},
        ]
    }

    resp = requests.post(f"{FABRIC_BASE_URL}/pipelines", headers=headers, json=payload)

    if resp.status_code == 409:
        print("⚠️  Pipeline already exists — fetching existing pipeline")
        list_resp = requests.get(f"{FABRIC_BASE_URL}/pipelines", headers=headers)
        list_resp.raise_for_status()
        for pl in list_resp.json().get("value", []):
            if pl["displayName"] == "PAB_DeployPipeline":
                print(f"  Found: {pl['id']}")
                return pl["id"]

    resp.raise_for_status()
    pipeline_id = resp.json()["id"]
    print(f"✅ Deployment pipeline created — ID: {pipeline_id}")
    return pipeline_id

# ── Assign Workspaces to Stages ───────────────────────────────────────────────
def assign_workspace_to_stage(token: str, pipeline_id: str, stage_order: int, workspace_id: str) -> None:
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    stage_names = {0: "Development", 1: "Test", 2: "Production"}

    payload = {"workspaceId": workspace_id}
    resp = requests.post(
        f"{FABRIC_BASE_URL}/pipelines/{pipeline_id}/stages/{stage_order}/assignWorkspace",
        headers=headers,
        json=payload
    )

    if resp.status_code in (200, 201, 204):
        print(f"  ✅ {stage_names[stage_order]} stage → workspace {workspace_id}")
    else:
        print(f"  ❌ Failed to assign {stage_names[stage_order]}: {resp.status_code} — {resp.text}")

# ── Get Pipeline Items (check pairing) ───────────────────────────────────────
def get_pipeline_stage_items(token: str, pipeline_id: str, stage_order: int) -> list:
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(
        f"{FABRIC_BASE_URL}/pipelines/{pipeline_id}/stages/{stage_order}/items",
        headers=headers
    )
    resp.raise_for_status()
    return resp.json().get("value", [])

# ── Deploy Between Stages ─────────────────────────────────────────────────────
def deploy_stage(token: str, pipeline_id: str, from_stage: int, to_stage: int, note: str = "") -> str:
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    stage_names = {0: "Dev", 1: "Test", 2: "Prod"}

    payload = {
        "sourceStageOrder":             from_stage,
        "targetStageOrder":             to_stage,
        "isBackwardDeployment":         False,
        "updateAppSettings":            True,
        "allowCreateArtifact":          True,
        "allowOverwriteArtifact":       True,
        "note": note or f"Deploy {stage_names[from_stage]} → {stage_names[to_stage]}"
    }

    resp = requests.post(
        f"{FABRIC_BASE_URL}/pipelines/{pipeline_id}/deploy",
        headers=headers,
        json=payload
    )

    if resp.status_code in (200, 202):
        operation_id = resp.json().get("operationId", "")
        print(f"  ✅ Deployment triggered — OperationId: {operation_id}")
        return operation_id
    else:
        print(f"  ❌ Deploy failed: {resp.status_code} — {resp.text}")
        resp.raise_for_status()
        return ""

# ── Poll Operation Status ─────────────────────────────────────────────────────
def wait_for_operation(token: str, operation_id: str, timeout_sec: int = 300) -> bool:
    if not operation_id:
        return True

    headers = {"Authorization": f"Bearer {token}"}
    elapsed = 0
    interval = 10

    while elapsed < timeout_sec:
        time.sleep(interval)
        elapsed += interval

        resp = requests.get(
            f"{FABRIC_BASE_URL}/operations/{operation_id}",
            headers=headers
        )
        status = resp.json().get("status", "Unknown")
        print(f"    [{elapsed}s] Status: {status}")

        if status.lower() == "succeeded":
            return True
        if status.lower() in ("failed", "cancelled"):
            print(f"    Operation details: {resp.json()}")
            return False

    print(f"    ⚠️ Timed out after {timeout_sec}s")
    return False

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("Fabric Deployment Pipeline Setup")
    print("=" * 60)

    token = get_token()
    print("✅ Token acquired\n")

    # 1. Create the deployment pipeline
    print("Creating deployment pipeline...")
    pipeline_id = create_pipeline(token)

    # 2. Assign workspaces to stages
    print("\nAssigning workspaces to pipeline stages...")
    assign_workspace_to_stage(token, pipeline_id, 0, WORKSPACE_IDS["PAB-Dev"])
    time.sleep(3)
    assign_workspace_to_stage(token, pipeline_id, 1, WORKSPACE_IDS["PAB-Test"])
    time.sleep(3)
    assign_workspace_to_stage(token, pipeline_id, 2, WORKSPACE_IDS["PAB-Prod"])

    # 3. Verify pairing
    print("\nVerifying item pairing in Development stage...")
    dev_items = get_pipeline_stage_items(token, pipeline_id, 0)
    print(f"  Items found in Dev stage: {len(dev_items)}")
    for item in dev_items:
        paired = "✅ Paired" if item.get("targetStageArtifactId") else "⚠️  Unpaired"
        print(f"  {paired} — {item.get('artifactDisplayName', 'Unknown')} [{item.get('artifactType', '?')}]")

    # 4. Initial deploy: Dev → Test
    print("\nDeploying Dev → Test (initial deployment)...")
    op_id = deploy_stage(token, pipeline_id, 0, 1, "Initial CI/CD demo deployment Dev→Test")
    success = wait_for_operation(token, op_id)
    print(f"  Dev→Test: {'✅ Done' if success else '❌ Failed'}")

    if success:
        print("\nDeploying Test → Prod (initial deployment)...")
        op_id = deploy_stage(token, pipeline_id, 1, 2, "Initial CI/CD demo deployment Test→Prod")
        success = wait_for_operation(token, op_id)
        print(f"  Test→Prod: {'✅ Done' if success else '❌ Failed'}")

    # Save pipeline ID
    with open("pipeline_id.txt", "w") as f:
        f.write(pipeline_id)
    print(f"\n✅ Pipeline ID saved to pipeline_id.txt: {pipeline_id}")
    print("Add this as FABRIC_PIPELINE_ID in GitHub repository secrets")

if __name__ == "__main__":
    main()
