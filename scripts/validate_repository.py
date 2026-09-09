"""Run lightweight validation against Fabric Git source files."""

import json
from pathlib import Path
import subprocess

import yaml


ROOT = Path(__file__).resolve().parents[1]
TEXT_SUFFIXES = {".json", ".pbir", ".pbism", ".pq", ".py", ".tmdl", ".yaml", ".yml"}
REQUIRED_ITEM_FILES = {
    "DataPipeline": ("pipeline-content.json",),
    "Dataflow": ("mashup.pq", "queryMetadata.json"),
    "Environment": ("Libraries/PublicLibraries/environment.yml", "Setting/Sparkcompute.yml"),
    "Lakehouse": ("lakehouse.metadata.json",),
    "Notebook": ("notebook-content.py",),
    "Report": ("definition.pbir",),
    "SemanticModel": ("definition.pbism",),
}
EXPECTED_PIPELINE = (
    ("NB_Seed_Bronze", "NB_01_Seed_Bronze", None),
    ("NB_Transform_Silver", "NB_02_Transform_Silver", "NB_Seed_Bronze"),
    ("NB_Aggregate_GOLD", "NB_03_Aggregate_Gold", "NB_Transform_Silver"),
)
EXPECTED_GOLD_SNIPPETS = {
    'spark.table("Silver_LH.dbo.production_conformed")',
    '.groupBy("date", "field")',
    '_write_gold(df_gold_prod, "production_daily")',
    '_write_gold(df_gold_cost, "cost_monthly")',
    '_write_gold(df_gold_sched, "schedule_summary")',
    '_write_gold(df_kpi_facts, "field_kpi_facts")',
    "directlake.update_direct_lake_model_connection(",
    "PowerBIRestClient._get_default_base_url = _powerbi_base_url",
    "labs.refresh_semantic_model(",
    'dataset=_SEMANTIC_MODEL, workspace=_ws_id, refresh_type="full"',
    "# PARAMETERS CELL ********************",
    "refresh_semantic_model = False",
}
EXPECTED_ENVIRONMENT_PIP = {
    "fabric-cicd==1.3.0",
    "semantic-link-labs==0.16.0",
    "semantic-link-sempy==0.14.1",
}
REMOVED_REFERENCES = ("DF_" + "Gold_PA", "SPNBased" + "RefreshNotAllowed")


def main() -> None:
    failures: list[str] = []
    tracked = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=ROOT,
        check=True,
        capture_output=True,
    ).stdout.decode("utf-8").split("\0")
    tracked_paths = [ROOT / path for path in tracked if path and (ROOT / path).is_file()]

    for path in tracked_paths:
        if path.suffix.lower() in TEXT_SUFFIXES or path.name == ".platform":
            try:
                content = path.read_text(encoding="utf-8-sig")
            except UnicodeDecodeError as error:
                failures.append(f"{path.relative_to(ROOT)}: {error}")
                continue
            if any(reference in content for reference in REMOVED_REFERENCES):
                failures.append(f"{path.relative_to(ROOT)}: contains a removed DF2 reference")

    python_paths = [
        path for path in tracked_paths
        if path.name == "notebook-content.py" or path.parent == ROOT / "scripts" and path.suffix == ".py"
    ]
    for path in python_paths:
        try:
            compile(path.read_text(encoding="utf-8"), str(path), "exec")
        except (SyntaxError, UnicodeDecodeError) as error:
            failures.append(f"{path.relative_to(ROOT)}: {error}")

    json_paths = [
        path for path in tracked_paths
        if path.suffix.lower() in {".json", ".pbir", ".pbism"} or path.name == ".platform"
    ]
    for path in json_paths:
        try:
            json.loads(path.read_text(encoding="utf-8-sig"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            failures.append(f"{path.relative_to(ROOT)}: {error}")

    yaml_paths = [path for path in tracked_paths if path.suffix.lower() in {".yaml", ".yml"}]
    for path in yaml_paths:
        try:
            yaml.safe_load(path.read_text(encoding="utf-8-sig"))
        except (UnicodeDecodeError, yaml.YAMLError) as error:
            failures.append(f"{path.relative_to(ROOT)}: {error}")

    environment_path = ROOT / "semanticlink.Environment" / "Libraries" / "PublicLibraries" / "environment.yml"
    environment = yaml.safe_load(environment_path.read_text(encoding="utf-8-sig"))
    environment_pip = {
        package
        for dependency in environment.get("dependencies", [])
        if isinstance(dependency, dict)
        for package in dependency.get("pip") or []
    }
    if environment_pip != EXPECTED_ENVIRONMENT_PIP:
        failures.append(
            f"{environment_path.relative_to(ROOT)}: expected exact pip pins "
            f"{sorted(EXPECTED_ENVIRONMENT_PIP)}, found {sorted(environment_pip)}"
        )

    seen_items: set[tuple[str, str]] = set()
    logical_items: dict[str, tuple[str, str]] = {}
    for platform_file in (path for path in tracked_paths if path.name == ".platform"):
        item_directory = platform_file.parent
        display_name, separator, item_type = item_directory.name.rpartition(".")
        if not separator:
            failures.append(f"{item_directory.relative_to(ROOT)}: item folder has no type suffix")
            continue
        identity = (item_type, display_name)
        if identity in seen_items:
            failures.append(f"{item_directory.relative_to(ROOT)}: duplicate {item_type} '{display_name}'")
        seen_items.add(identity)
        try:
            platform = json.loads(platform_file.read_text(encoding="utf-8-sig"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        metadata = platform.get("metadata", {})
        if metadata.get("type") != item_type or metadata.get("displayName") != display_name:
            failures.append(
                f"{platform_file.relative_to(ROOT)}: metadata does not match its item folder"
            )
        logical_id = platform.get("config", {}).get("logicalId")
        if not logical_id:
            failures.append(f"{platform_file.relative_to(ROOT)}: missing config.logicalId")
        elif logical_id in logical_items:
            failures.append(f"{platform_file.relative_to(ROOT)}: duplicate logicalId {logical_id}")
        else:
            logical_items[logical_id] = identity
        for relative_file in REQUIRED_ITEM_FILES.get(item_type, ()):
            if not (item_directory / relative_file).is_file():
                failures.append(
                    f"{item_directory.relative_to(ROOT)}: missing required file {relative_file}"
                )

    if any(item_type == "Dataflow" for item_type, _display_name in seen_items):
        failures.append("Dataflow item found: this solution's Gold path must remain notebook-only")

    pipeline_path = ROOT / "Seed_Data" / "PL_Refresh_Master.DataPipeline" / "pipeline-content.json"
    pipeline = json.loads(pipeline_path.read_text(encoding="utf-8-sig"))
    activities = pipeline.get("properties", {}).get("activities", [])
    if len(activities) != len(EXPECTED_PIPELINE):
        failures.append(f"{pipeline_path.relative_to(ROOT)}: expected three notebook activities")
    else:
        for activity, (expected_name, expected_notebook, expected_dependency) in zip(
            activities[:3], EXPECTED_PIPELINE, strict=True
        ):
            if activity.get("name") != expected_name or activity.get("type") != "TridentNotebook":
                failures.append(
                    f"{pipeline_path.relative_to(ROOT)}: expected notebook activity {expected_name}"
                )
                continue
            properties = activity.get("typeProperties", {})
            if logical_items.get(properties.get("notebookId")) != ("Notebook", expected_notebook):
                failures.append(
                    f"{pipeline_path.relative_to(ROOT)}: {expected_name} has the wrong notebook logicalId"
                )
            if properties.get("workspaceId") != "00000000-0000-0000-0000-000000000000":
                failures.append(
                    f"{pipeline_path.relative_to(ROOT)}: {expected_name} must use Fabric's current-workspace placeholder"
                )
            if "externalReferences" in activity:
                failures.append(
                    f"{pipeline_path.relative_to(ROOT)}: {expected_name} must not use an external Notebook connection"
                )
            dependencies = activity.get("dependsOn", [])
            actual_dependency = dependencies[0].get("activity") if len(dependencies) == 1 else None
            if actual_dependency != expected_dependency or (expected_dependency is None and dependencies):
                failures.append(
                    f"{pipeline_path.relative_to(ROOT)}: {expected_name} has the wrong dependency"
                )

        aggregate_parameters = activities[2].get("typeProperties", {}).get("parameters", {})
        if "refresh_semantic_model" in aggregate_parameters:
            failures.append(
                f"{pipeline_path.relative_to(ROOT)}: NB_Aggregate_GOLD must not run the Semantic Link refresh"
            )

    gold_path = ROOT / "Gold" / "NB_03_Aggregate_Gold.Notebook" / "notebook-content.py"
    gold_source = gold_path.read_text(encoding="utf-8-sig")
    for snippet in sorted(EXPECTED_GOLD_SNIPPETS):
        if snippet not in gold_source:
            failures.append(f"{gold_path.relative_to(ROOT)}: missing required logic: {snippet}")

    if failures:
        raise SystemExit("Repository validation failed:\n" + "\n".join(failures))
    print(
        f"Validated {len(tracked_paths)} tracked files, {len(python_paths)} Python files, "
        f"{len(json_paths)} JSON files, {len(yaml_paths)} YAML files, and "
        f"{len(seen_items)} Fabric item folders"
    )


if __name__ == "__main__":
    main()