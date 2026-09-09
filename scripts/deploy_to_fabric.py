"""Deploy Fabric Git item definitions from GitHub Actions using fabric-cicd."""

from __future__ import annotations

import argparse
import atexit
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
from typing import Any


FABRIC_API = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
NOTEBOOK_CONNECTION_NAME = "FabricCICD Notebook Workspace Identity"
CONNECTION_ID_PLACEHOLDER = "11111111-1111-1111-1111-111111111111"
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-workspace", required=True)
    parser.add_argument("--dev-workspace", required=True)
    parser.add_argument("--environment", default="Production")
    parser.add_argument("--repository-directory", default=".")
    parser.add_argument("--git-compare-ref", default="HEAD~1")
    parser.add_argument("--full-deploy", action="store_true")
    parser.add_argument("--remove-orphans", action="store_true")
    parser.add_argument("--recreate-analytics-items", action="store_true")
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

    def ensure_workspace_identity(self, workspace_id: str) -> None:
        import requests

        for attempt in range(1, 5):
            try:
                self.post(f"/workspaces/{workspace_id}/provisionIdentity")
                break
            except requests.HTTPError as error:
                response_body = error.response.json() if error.response.content else {}
                if response_body.get("errorCode") == "WorkspaceIdentityAlreadyExists":
                    break
                if error.response.status_code not in {429, 500, 502, 503, 504} or attempt == 4:
                    raise
                time.sleep(2 ** (attempt - 1))
        print(f"Workspace identity is provisioned for {workspace_id}")

    def ensure_workspace_identity_role(self, workspace_id: str) -> None:
        workspace = self.get(f"/workspaces/{workspace_id}")
        identity = workspace.get("workspaceIdentity", {})
        principal_id = identity.get("servicePrincipalId")
        if not principal_id:
            raise ValueError(f"Workspace {workspace_id} has no provisioned identity principal")

        assignments = self.get(f"/workspaces/{workspace_id}/roleAssignments").get("value", [])
        assignment = next(
            (
                item
                for item in assignments
                if item.get("principal", {}).get("id") == principal_id
            ),
            None,
        )
        if assignment:
            print(
                f"Workspace identity already has {assignment['role']} access to {workspace_id}"
            )
            return

        self.post(
            f"/workspaces/{workspace_id}/roleAssignments",
            {
                "principal": {"id": principal_id, "type": "ServicePrincipal"},
                "role": "Contributor",
            },
        )
        print(f"Granted Workspace Identity Contributor access to {workspace_id}")

    def ensure_notebook_workspace_identity_connection(self, workspace_id: str) -> str:
        connections = self.get("/connections").get("value", [])
        named = [
            connection
            for connection in connections
            if connection.get("displayName") == NOTEBOOK_CONNECTION_NAME
        ]
        for connection in named:
            if (
                connection.get("connectionDetails", {}).get("type") == "Notebook"
                and connection.get("credentialDetails", {}).get("credentialType")
                == "WorkspaceIdentity"
            ):
                print(
                    f"Reusing Notebook Workspace Identity connection: "
                    f"{NOTEBOOK_CONNECTION_NAME} ({connection['id']})"
                )
                return connection["id"]
        if named:
            raise ValueError(
                f"Connection '{NOTEBOOK_CONNECTION_NAME}' exists with incompatible type or credentials"
            )

        connection = self.post(
            "/connections",
            {
                "connectivityType": "ShareableCloud",
                "displayName": NOTEBOOK_CONNECTION_NAME,
                "connectionDetails": {
                    "type": "Notebook",
                    "creationMethod": "Notebook.Actions",
                    "parameters": [],
                },
                "privacyLevel": "Organizational",
                "credentialDetails": {
                    "singleSignOnType": "None",
                    "connectionEncryption": "NotEncrypted",
                    "skipTestConnection": False,
                    "credentials": {"credentialType": "WorkspaceIdentity"},
                },
            },
        )
        connection_id = connection.get("id")
        if not connection_id:
            raise ValueError("Fabric created the Power BI connection without returning its ID")
        print(f"Created Notebook Workspace Identity connection: {connection_id}")
        return connection_id


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
        item_id = target_by_type_name.get((item_type, display_name))
        if item_id is None:
            print(f"Git-removed item already absent: {display_name}.{item_type}")
            continue
        api.delete(f"/workspaces/{target_workspace_id}/items/{item_id}")
        print(f"Deleted Git-removed item: {display_name}.{item_type} ({item_id})")


def recreate_analytics_items(target_workspace_id: str, api: FabricApi) -> None:
    target_items = api.get(f"/workspaces/{target_workspace_id}/items").get("value", [])
    target_by_type_name = {
        (item["type"], item["displayName"]): item["id"] for item in target_items
    }
    for item_type, display_name in (
        ("Report", "Gold_Dashboard"),
        ("SemanticModel", "Gold_SM"),
    ):
        item_id = target_by_type_name.get((item_type, display_name))
        if item_id is None:
            print(f"SPN recreation: {display_name}.{item_type} is already absent")
            continue
        api.delete(f"/workspaces/{target_workspace_id}/items/{item_id}")
        print(f"SPN recreation: deleted {display_name}.{item_type} ({item_id})")


def generate_parameters(
    repository_directory: Path,
    repository_items: list[tuple[str, str, Path]],
    dev_workspace_id: str,
    environment: str,
    api: FabricApi,
    notebook_connection_id: str,
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
    rules.append(
        {
            "find_value": CONNECTION_ID_PLACEHOLDER,
            "replace_value": replacement(notebook_connection_id),
            "item_type": ["DataPipeline"],
        }
    )

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

    return sorted(selected)


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
    api.ensure_workspace_identity(target_workspace_id)
    api.ensure_workspace_identity_role(target_workspace_id)
    notebook_connection_id = api.ensure_notebook_workspace_identity_connection(
        target_workspace_id
    )
    repository_items = discover_items(repository_directory)
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
        repository_items,
        dev_workspace_id,
        args.environment,
        api,
        notebook_connection_id,
    )
    workspace = FabricWorkspace(
        workspace_id=target_workspace_id,
        repository_directory=str(repository_directory),
        item_type_in_scope=item_types,
        environment=args.environment,
        token_credential=credential,
    )
    print(f"Comparing {len(repository_items)} source items with {args.target_workspace}")
    if args.full_deploy:
        if args.recreate_analytics_items:
            raise ValueError("--recreate-analytics-items cannot be combined with --full-deploy")
        print("Full deployment requested")
        publish_all_items(workspace)
    else:
        if args.recreate_analytics_items:
            recreate_analytics_items(target_workspace_id, api)
            items_to_publish = ["Gold_SM.SemanticModel", "Gold_Dashboard.Report"]
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