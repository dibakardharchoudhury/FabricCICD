"""Deploy Fabric Git item definitions from GitHub Actions using fabric-cicd."""

from __future__ import annotations

import argparse
import base64
import json
import os
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit


FABRIC_API = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
EXCLUDED_ITEM_NAMES = {"NB_04_Deploy_NOTSECURE"}
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
    parser.add_argument("--pipeline-run-client-id")
    parser.add_argument("--verify-pipeline-runs", action="store_true")
    parser.add_argument("--full-deploy", action="store_true")
    parser.add_argument("--remove-orphans", action="store_true")
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

    def patch(self, path: str, payload: dict) -> dict:
        import requests

        token = self.credential.get_token(FABRIC_SCOPE).token
        response = requests.patch(
            f"{FABRIC_API}{path}",
            headers={"Authorization": f"Bearer {token}"},
            json=payload,
            timeout=60,
        )
        response.raise_for_status()
        return response.json()

    def run_item_job(self, workspace_id: str, item_id: str, job_type: str) -> tuple[str, int]:
        import requests

        token = self.credential.get_token(FABRIC_SCOPE).token
        response = requests.post(
            f"{FABRIC_API}/workspaces/{workspace_id}/items/{item_id}/jobs/instances",
            headers={"Authorization": f"Bearer {token}"},
            params={"jobType": job_type},
            timeout=60,
        )
        response.raise_for_status()
        location = response.headers.get("Location")
        if not location:
            raise RuntimeError("Fabric accepted the job but returned no Location header")
        retry_after = int(response.headers.get("Retry-After", "5"))
        return urlsplit(location).path.removeprefix("/v1"), retry_after

    def verify_application_identity(self, expected_client_id: str) -> None:
        token = self.credential.get_token(FABRIC_SCOPE).token
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload))
        actual_client_id = claims.get("appid") or claims.get("azp")
        if not actual_client_id or actual_client_id.lower() != expected_client_id.lower():
            raise ValueError(
                "Pipeline ownership requires the configured service principal token; "
                f"expected {expected_client_id}, received {actual_client_id or '(user token)'}"
            )

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
        directories[:] = [name for name in directories if name != ".git" and not name.startswith(".")]
        item_directories: list[str] = []
        for directory in directories:
            display_name, separator, item_type = directory.rpartition(".")
            if (
                separator
                and item_type in SUPPORTED_ITEM_TYPES
                and display_name not in EXCLUDED_ITEM_NAMES
            ):
                items.append((item_type, display_name, Path(root, directory)))
                item_directories.append(directory)
        directories[:] = [name for name in directories if name not in item_directories]
    return items


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
    publish_types = sorted({item_type for item_type in items_by_type if item_type != "VariableLibrary"})
    workspace_rule = {
        "find_value": dev_workspace_id,
        "replace_value": replacement("$workspace.$id"),
    }
    rules.append(workspace_rule)

    for item_type in ("Notebook", "Dataflow", "SemanticModel"):
        for display_name in sorted(items_by_type.get(item_type, [])):
            try:
                dev_item_id = api.resolve_item_id(dev_workspace_id, display_name, item_type)
            except ValueError:
                continue
            rules.append(
                {
                    "find_value": dev_item_id,
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
        if item.rsplit(".", 1)[0] not in EXCLUDED_ITEM_NAMES
    )


def set_pipeline_run_identity(
    repository_items: list[tuple[str, str, Path]],
    workspace_ids: list[str],
    api: FabricApi,
    client_id: str,
) -> None:
    api.verify_application_identity(client_id)
    pipeline_names = sorted(
        display_name
        for item_type, display_name, _path in repository_items
        if item_type == "DataPipeline"
    )
    description = f"Notebook activities run as service principal {client_id}."
    for workspace_id in dict.fromkeys(workspace_ids):
        for display_name in pipeline_names:
            pipeline_id = api.resolve_item_id(workspace_id, display_name, "DataPipeline")
            api.patch(
                f"/workspaces/{workspace_id}/dataPipelines/{pipeline_id}",
                {"description": description},
            )
            print(
                f"Set {display_name} LastModifiedBy to service principal {client_id} "
                f"in workspace {workspace_id}"
            )


def verify_pipeline_runs(
    repository_items: list[tuple[str, str, Path]],
    workspace_ids: list[str],
    api: FabricApi,
    client_id: str,
) -> None:
    api.verify_application_identity(client_id)
    pipeline_names = sorted(
        display_name
        for item_type, display_name, _path in repository_items
        if item_type == "DataPipeline"
    )
    terminal_states = {"Completed", "Failed", "Cancelled", "Deduped"}
    for workspace_id in dict.fromkeys(workspace_ids):
        for display_name in pipeline_names:
            pipeline_id = api.resolve_item_id(workspace_id, display_name, "DataPipeline")
            job_path, retry_after = api.run_item_job(workspace_id, pipeline_id, "Pipeline")
            print(f"Started {display_name} as service principal {client_id} in workspace {workspace_id}")
            time.sleep(retry_after)
            while True:
                job = api.get(job_path)
                status = job.get("status")
                if status in terminal_states:
                    break
                time.sleep(30)
            if status != "Completed":
                raise RuntimeError(
                    f"{display_name} finished with status {status} in workspace {workspace_id}: "
                    f"{json.dumps(job.get('failureReason'))}"
                )
            print(
                f"Completed {display_name} as service principal {client_id} "
                f"in workspace {workspace_id}; job {job.get('id')}"
            )


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
    repository_items = discover_items(repository_directory)
    item_types = sorted(
        {item_type for item_type, _name, _path in repository_items if item_type != "VariableLibrary"}
    )
    if not item_types:
        raise ValueError(f"No supported Fabric item folders were found under {repository_directory}")

    generate_parameters(
        repository_directory,
        repository_items,
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
    print("Excluded item names: " + ", ".join(sorted(EXCLUDED_ITEM_NAMES)))
    if args.full_deploy:
        print("Full deployment requested")
        publish_all_items(workspace, item_name_exclude_regex=r"^NB_04_Deploy_NOTSECURE$")
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
    if args.remove_orphans:
        unpublish_all_orphan_items(workspace)
    if args.pipeline_run_client_id:
        set_pipeline_run_identity(
            repository_items,
            [dev_workspace_id, target_workspace_id],
            api,
            args.pipeline_run_client_id,
        )
    if args.verify_pipeline_runs:
        if not args.pipeline_run_client_id:
            raise ValueError("--verify-pipeline-runs requires --pipeline-run-client-id")
        verify_pipeline_runs(
            repository_items,
            [dev_workspace_id, target_workspace_id],
            api,
            args.pipeline_run_client_id,
        )
    print(f"Deployment to {args.target_workspace} completed")


if __name__ == "__main__":
    main()