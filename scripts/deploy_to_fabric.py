"""Deploy Fabric Git item definitions from GitHub Actions using fabric-cicd."""

from __future__ import annotations

import argparse
import atexit
import base64
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
from typing import Any


FABRIC_API = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
SUPPORTED_ITEM_TYPES = {
    "ApacheAirflowJob",
    "CopyJob",
    "DataAgent",
    "Dataflow",
    "DataPipeline",
    "Environment",
    "Eventhouse",
    "Eventstream",
    "GraphQLApi",
    "KQLDashboard",
    "KQLDatabase",
    "KQLQueryset",
    "Lakehouse",
    "MirroredDatabase",
    "MountedDataFactory",
    "Notebook",
    "Reflex",
    "Report",
    "SemanticModel",
    "SparkJobDefinition",
    "SQLDatabase",
    "UserDataFunction",
    "VariableLibrary",
    "Warehouse",
}
ADMIN_OWNED_ITEMS = {
    ("Notebook", "NB_04_SemanticModelReBindRefresh"),
    ("DataPipeline", "PL_SemanticModel_Rebind_Refresh"),
}
DEPLOYMENT_EXCLUDED_ITEMS = ADMIN_OWNED_ITEMS
ADMIN_UPN = "admin@mngenvmcap218279.onmicrosoft.com"
ADMIN_OBJECT_ID = "7ab1a6b2-d2e6-41b8-92ba-1fb3a8ba5bc0"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-workspace", required=True)
    parser.add_argument("--dev-workspace", required=True)
    parser.add_argument("--environment", default="Production")
    parser.add_argument("--repository-directory", default=".")
    parser.add_argument("--git-compare-ref", default="HEAD~1")
    parser.add_argument("--full-deploy", action="store_true")
    parser.add_argument("--remove-orphans", action="store_true")
    parser.add_argument("--admin-owned-only", action="store_true")
    return parser.parse_args()


class FabricApi:
    def __init__(self, credential: Any) -> None:
        self.credential = credential

    def get(self, path: str) -> dict:
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        token = self.credential.get_token(FABRIC_SCOPE).token
        retry = Retry(
            total=4,
            backoff_factor=1,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods={"GET"},
            respect_retry_after_header=True,
        )
        session = requests.Session()
        session.mount("https://", HTTPAdapter(max_retries=retry))
        response = session.get(
            f"{FABRIC_API}{path}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=60,
        )
        response.raise_for_status()
        return response.json()

    def post(self, path: str, body: dict | None = None) -> dict:
        import requests

        token = self.credential.get_token(FABRIC_SCOPE).token
        response = requests.post(
            f"{FABRIC_API}{path}",
            headers={"Authorization": f"Bearer {token}"},
            json=body,
            timeout=60,
        )
        response.raise_for_status()
        if response.status_code == 202:
            operation_url = response.headers.get("Location")
            if not operation_url:
                raise ValueError(f"Fabric accepted POST {path} without an operation URL")
            return self.wait_for_operation(operation_url)
        return response.json() if response.content else {}

    def wait_for_operation(self, operation_url: str) -> dict:
        import requests

        for _attempt in range(60):
            token = self.credential.get_token(FABRIC_SCOPE).token
            response = requests.get(
                operation_url,
                headers={"Authorization": f"Bearer {token}"},
                timeout=60,
            )
            response.raise_for_status()
            operation = response.json()
            status = operation.get("status")
            if status == "Succeeded":
                return operation
            if status in {"Failed", "Cancelled"}:
                raise RuntimeError(f"Fabric operation {status}: {operation.get('error')}")
            time.sleep(2)
        raise TimeoutError(f"Fabric operation did not complete: {operation_url}")

    def delete(self, path: str) -> bool:
        import requests

        token = self.credential.get_token(FABRIC_SCOPE).token
        response = requests.delete(
            f"{FABRIC_API}{path}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=60,
        )
        if response.status_code == 404:
            return False
        response.raise_for_status()
        return True

    def resolve_workspace_id(self, display_name: str) -> str:
        workspaces = self.get("/workspaces").get("value", [])
        match = next(
            (workspace["id"] for workspace in workspaces if workspace["displayName"] == display_name),
            None,
        )
        if not match:
            visible = ", ".join(sorted(workspace["displayName"] for workspace in workspaces))
            raise ValueError(f"Workspace '{display_name}' is not visible. Visible: {visible or '(none)'}")
        return match

    def resolve_item_id(self, workspace_id: str, display_name: str, item_type: str) -> str:
        items = self.get(f"/workspaces/{workspace_id}/items?type={item_type}").get("value", [])
        match = next((item["id"] for item in items if item["displayName"] == display_name), None)
        if not match:
            raise ValueError(f"{item_type} '{display_name}' was not found in workspace {workspace_id}")
        return match

def discover_items(repository_directory: Path) -> list[tuple[str, str, Path]]:
    items: list[tuple[str, str, Path]] = []
    for root, directories, _files in os.walk(repository_directory):
        directories[:] = [
            name
            for name in directories
            if name != ".git"
            and not name.startswith(".")
        ]
        item_directories: list[str] = []
        for directory in directories:
            display_name, separator, item_type = directory.rpartition(".")
            if (
                separator
                and item_type in SUPPORTED_ITEM_TYPES
                and (Path(root) / directory / ".platform").is_file()
            ):
                items.append((item_type, display_name, Path(root, directory)))
                item_directories.append(directory)
        directories[:] = [name for name in directories if name not in item_directories]
    return items


def remove_generated_item_artifacts(
    repository_items: list[tuple[str, str, Path]],
) -> None:
    for _item_type, _display_name, item_path in repository_items:
        for cache_directory in item_path.rglob("__pycache__"):
            shutil.rmtree(cache_directory)
        for pattern in ("*.pyc", "*.pyo"):
            for artifact in item_path.rglob(pattern):
                artifact.unlink()


def discover_deleted_items(repository_directory: Path, git_compare_ref: str) -> list[tuple[str, str]]:
    result = subprocess.run(
        [
            "git",
            "diff",
            "--name-only",
            "--diff-filter=D",
            git_compare_ref,
            "HEAD",
            "--",
        ],
        cwd=repository_directory,
        check=True,
        capture_output=True,
        text=True,
    )
    deleted_items: set[tuple[str, str]] = set()
    for changed_path in result.stdout.splitlines():
        if Path(changed_path).name != ".platform":
            continue
        for part in Path(changed_path).parts:
            display_name, separator, item_type = part.rpartition(".")
            if separator and item_type in SUPPORTED_ITEM_TYPES:
                deleted_items.add((item_type, display_name))
                break
    return sorted(deleted_items)


def delete_removed_items(
    repository_directory: Path,
    git_compare_ref: str,
    target_workspace_id: str,
    api: FabricApi,
) -> None:
    deleted_items = discover_deleted_items(repository_directory, git_compare_ref)
    if not deleted_items:
        print("No Git-removed Fabric items to delete")
        return

    target_items = api.get(f"/workspaces/{target_workspace_id}/items").get("value", [])
    target_by_type_name = {
        (item["type"], item["displayName"]): item["id"] for item in target_items
    }
    for item_type, display_name in deleted_items:
        if (item_type, display_name) in DEPLOYMENT_EXCLUDED_ITEMS:
            print(f"Skipping deployment-excluded deletion: {display_name}.{item_type}")
            continue
        item_id = target_by_type_name.get((item_type, display_name))
        if item_id is None:
            print(f"Git-removed item already absent: {display_name}.{item_type}")
            continue
        api.delete(f"/workspaces/{target_workspace_id}/items/{item_id}")
        print(f"Deleted Git-removed item: {display_name}.{item_type} ({item_id})")


def generate_parameters(
    repository_directory: Path,
    repository_items: list[tuple[str, str, Path]],
    dev_workspace_id: str,
    environment: str,
    api: FabricApi,
) -> None:
    import yaml

    items_by_type: dict[str, list[str]] = {}
    for item_type, display_name, _path in repository_items:
        items_by_type.setdefault(item_type, []).append(display_name)

    environments = list(dict.fromkeys(["Development", "Production", environment]))

    def replacement(value: str) -> dict[str, str]:
        return {name: value for name in environments}

    rules: list[dict] = []
    workspace_rule = {
        "find_value": dev_workspace_id,
        "replace_value": replacement("$workspace.$id"),
    }
    rules.append(workspace_rule)
    for item_type, display_name, item_path in repository_items:
        if item_type in {"Notebook", "Dataflow", "SemanticModel"}:
            platform = json.loads((item_path / ".platform").read_text(encoding="utf-8-sig"))
            logical_id = platform.get("config", {}).get("logicalId")
            if not logical_id:
                raise ValueError(f"{item_path}: missing config.logicalId")
            rules.append(
                {
                    "find_value": logical_id,
                    "replace_value": replacement(f"$items.{item_type}.{display_name}.$id"),
                    "item_type": ["DataPipeline"],
                }
            )

    for display_name in sorted(items_by_type.get("Lakehouse", [])):
        try:
            dev_item_id = api.resolve_item_id(dev_workspace_id, display_name, "Lakehouse")
        except ValueError:
            continue
        rules.append(
            {
                "find_value": dev_item_id,
                "replace_value": replacement(f"$items.Lakehouse.{display_name}.$id"),
                    "item_type": ["Notebook", "Dataflow", "SemanticModel"],
            }
        )

    parameter_file = repository_directory / "parameter.yml"
    parameter_data = {"find_replace": rules}
    parameter_file.write_text(
        "# Generated by scripts/deploy_to_fabric.py; do not edit.\n"
        + yaml.safe_dump(parameter_data, sort_keys=False),
        encoding="utf-8",
    )
    if yaml.safe_load(parameter_file.read_text(encoding="utf-8")) != parameter_data:
        raise ValueError(f"Generated parameter file failed round-trip validation: {parameter_file}")
    print(f"Generated and validated {parameter_file} with {len(rules)} replacement rules")


def select_items_to_publish(
    repository_items: list[tuple[str, str, Path]],
    dev_workspace_id: str,
    target_workspace_id: str,
    api: FabricApi,
    repository_directory: Path,
    git_compare_ref: str,
) -> list[str]:
    from fabric_cicd import get_changed_items

    selected = set(get_changed_items(repository_directory, git_compare_ref=git_compare_ref))
    target_items = api.get(f"/workspaces/{target_workspace_id}/items").get("value", [])
    target_by_type_name = {(item["type"], item["displayName"]): item for item in target_items}

    for item_type, display_name, _path in repository_items:
        target_item = target_by_type_name.get((item_type, display_name))
        if target_item is None:
            selected.add(f"{display_name}.{item_type}")
            continue
        if item_type != "SemanticModel":
            continue
        connections = api.get(
            f"/workspaces/{target_workspace_id}/items/{target_item['id']}/connections"
        ).get("value", [])
        if any(dev_workspace_id in connection.get("connectionDetails", {}).get("path", "") for connection in connections):
            selected.add(f"{display_name}.{item_type}")
            print(f"Selected {display_name}.{item_type}: its connection still points to Dev")

    return sorted(
        item
        for item in selected
        if tuple(reversed(item.rsplit(".", 1))) not in DEPLOYMENT_EXCLUDED_ITEMS
    )


def verify_admin_identity(credential: Any) -> None:
    token = credential.get_token(FABRIC_SCOPE).token
    encoded_claims = token.split(".")[1]
    claims = json.loads(base64.urlsafe_b64decode(encoded_claims + "=" * (-len(encoded_claims) % 4)))
    upn = claims.get("upn") or claims.get("preferred_username")
    if claims.get("oid", "").lower() != ADMIN_OBJECT_ID or upn.lower() != ADMIN_UPN.lower():
        raise PermissionError(f"Administrator-owned deployment requires Azure CLI user {ADMIN_UPN}")
    print(f"Verified administrator identity: {upn} ({claims['oid']})")


def main() -> None:
    from azure.identity import AzureCliCredential
    from fabric_cicd import (
        FabricWorkspace,
        append_feature_flag,
        publish_all_items,
        unpublish_all_orphan_items,
    )

    args = parse_args()
    repository_directory = Path(args.repository_directory).resolve()
    credential = AzureCliCredential()
    api = FabricApi(credential)
    dev_workspace_id = api.resolve_workspace_id(args.dev_workspace)
    target_workspace_id = api.resolve_workspace_id(args.target_workspace)
    print(f"Dev workspace: {args.dev_workspace} ({dev_workspace_id})")
    print(f"Target workspace: {args.target_workspace} ({target_workspace_id})")
    all_repository_items = discover_items(repository_directory)
    repository_items = all_repository_items
    if args.admin_owned_only:
        verify_admin_identity(credential)
        repository_items = [
            item for item in repository_items if (item[0], item[1]) in ADMIN_OWNED_ITEMS
        ]
    else:
        repository_items = [
            item for item in repository_items if (item[0], item[1]) not in DEPLOYMENT_EXCLUDED_ITEMS
        ]
    remove_generated_item_artifacts(repository_items)
    item_types = sorted(
        {item_type for item_type, _name, _path in repository_items if item_type != "VariableLibrary"}
    )
    if not item_types:
        raise ValueError(f"No supported Fabric item folders were found under {repository_directory}")

    parameter_file = repository_directory / "parameter.yml"
    atexit.register(parameter_file.unlink, missing_ok=True)
    generate_parameters(
        repository_directory,
        all_repository_items,
        dev_workspace_id,
        args.environment,
        api,
    )
    workspace = FabricWorkspace(
        workspace_id=target_workspace_id,
        repository_directory=str(repository_directory),
        item_type_in_scope=item_types,
        environment=args.environment,
        token_credential=credential,
    )
    print(f"Comparing {len(repository_items)} source items with {args.target_workspace}")
    if args.admin_owned_only:
        append_feature_flag("enable_experimental_features")
        append_feature_flag("enable_items_to_include")
        admin_items = sorted(f"{name}.{item_type}" for item_type, name, _path in repository_items)
        print(f"Publishing administrator-owned items as {ADMIN_UPN}: " + ", ".join(admin_items))
        publish_all_items(workspace, items_to_include=admin_items)
    elif args.full_deploy:
        print("Full deployment requested")
        publish_all_items(workspace)
    else:
        items_to_publish = select_items_to_publish(
            repository_items,
            dev_workspace_id,
            target_workspace_id,
            api,
            repository_directory,
            args.git_compare_ref,
        )
        if items_to_publish:
            append_feature_flag("enable_experimental_features")
            append_feature_flag("enable_items_to_include")
            print("Publishing changed, missing, or environment-drifted items: " + ", ".join(items_to_publish))
            publish_all_items(workspace, items_to_include=items_to_publish)
        else:
            print("No changed, missing, or environment-drifted Fabric items to publish")
    delete_removed_items(
        repository_directory,
        args.git_compare_ref,
        target_workspace_id,
        api,
    )
    if args.remove_orphans:
        unpublish_all_orphan_items(workspace)
    print(f"Deployment to {args.target_workspace} completed")


if __name__ == "__main__":
    main()