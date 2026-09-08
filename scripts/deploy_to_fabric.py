"""Deploy Fabric Git item definitions from GitHub Actions using fabric-cicd."""

from __future__ import annotations

import argparse
import glob
import os
import re
import sys
import types
from pathlib import Path
from typing import Any


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
    non_model_types = [item_type for item_type in publish_types if item_type != "SemanticModel"]
    if "SemanticModel" in publish_types and non_model_types:
        workspace_rule["item_type"] = non_model_types
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
                "item_type": ["Notebook", "Dataflow"],
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


def rebind_direct_lake_models(
    repository_items: list[tuple[str, str, Path]],
    dev_workspace_id: str,
    target_workspace_id: str,
    api: FabricApi,
    credential: Any,
) -> None:
    sys.modules.setdefault("notebookutils", types.ModuleType("notebookutils"))
    from sempy_labs._authentication import ServicePrincipalTokenProvider, token_provider
    from sempy_labs import directlake

    dev_lakehouses = api.get(f"/workspaces/{dev_workspace_id}/items?type=Lakehouse").get("value", [])
    dev_lakehouse_names = {item["id"]: item["displayName"] for item in dev_lakehouses}

    provider_context = token_provider.set(ServicePrincipalTokenProvider(credential))
    try:
        for item_type, model_name, model_directory in repository_items:
            if item_type != "SemanticModel":
                continue
            definition = "\n".join(
                Path(file_name).read_text(encoding="utf-8")
                for file_name in glob.glob(str(model_directory / "**" / "*.tmdl"), recursive=True)
            )
            source_match = re.search(
                r"onelake\.dfs\.fabric\.microsoft\.com/[0-9a-fA-F-]{36}/([0-9a-fA-F-]{36})",
                definition,
            )
            if not source_match:
                continue
            lakehouse_name = dev_lakehouse_names.get(source_match.group(1))
            if not lakehouse_name:
                raise ValueError(
                    f"Could not resolve the Dev lakehouse used by semantic model '{model_name}'"
                )
            directlake.update_direct_lake_model_connection(
                dataset=model_name,
                workspace=target_workspace_id,
                source=api.resolve_item_id(target_workspace_id, lakehouse_name, "Lakehouse"),
                source_type="Lakehouse",
                source_workspace=target_workspace_id,
                use_sql_endpoint=False,
            )
            print(f"Rebound {model_name} to {lakehouse_name} in the target workspace")
    finally:
        token_provider.reset(provider_context)


def main() -> None:
    from azure.identity import AzureCliCredential
    from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

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
    publish_all_items(workspace, item_name_exclude_regex=r"^NB_04_Deploy_NOTSECURE$")
    if args.remove_orphans:
        unpublish_all_orphan_items(workspace)
    rebind_direct_lake_models(
        repository_items,
        dev_workspace_id,
        target_workspace_id,
        api,
        credential,
    )
    print(f"Deployment to {args.target_workspace} completed")


if __name__ == "__main__":
    main()