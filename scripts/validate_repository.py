"""Run lightweight validation against Fabric Git source files."""

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
EXCLUDED_ITEM_DIRECTORIES = {"NB_04_Deploy_NOTSECURE.Notebook"}
REQUIRED_ITEM_FILES = {
    "DataPipeline": ("pipeline-content.json",),
    "Dataflow": ("mashup.pq", "queryMetadata.json"),
    "Environment": ("Libraries/PublicLibraries/environment.yml", "Setting/Sparkcompute.yml"),
    "Lakehouse": ("lakehouse.metadata.json",),
    "Notebook": ("notebook-content.py",),
    "Report": ("definition.pbir",),
    "SemanticModel": ("definition.pbism",),
}


def is_excluded(path: Path) -> bool:
    return any(part in EXCLUDED_ITEM_DIRECTORIES for part in path.parts)


def main() -> None:
    failures: list[str] = []
    python_paths = [path for path in ROOT.rglob("notebook-content.py") if not is_excluded(path)]
    python_paths += list((ROOT / "scripts").glob("*.py"))
    for path in python_paths:
        try:
            compile(path.read_text(encoding="utf-8"), str(path), "exec")
        except (SyntaxError, UnicodeDecodeError) as error:
            failures.append(f"{path.relative_to(ROOT)}: {error}")

    json_paths = [path for path in ROOT.rglob("*.json") if not is_excluded(path)]
    json_paths += [path for path in ROOT.rglob(".platform") if not is_excluded(path)]
    for path in json_paths:
        try:
            json.loads(path.read_text(encoding="utf-8-sig"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            failures.append(f"{path.relative_to(ROOT)}: {error}")

    seen_items: set[tuple[str, str]] = set()
    for platform_file in (path for path in ROOT.rglob(".platform") if not is_excluded(path)):
        item_directory = platform_file.parent
        display_name, separator, item_type = item_directory.name.rpartition(".")
        if not separator:
            failures.append(f"{item_directory.relative_to(ROOT)}: item folder has no type suffix")
            continue
        identity = (item_type, display_name)
        if identity in seen_items:
            failures.append(f"{item_directory.relative_to(ROOT)}: duplicate {item_type} '{display_name}'")
        seen_items.add(identity)
        for relative_file in REQUIRED_ITEM_FILES.get(item_type, ()):
            if not (item_directory / relative_file).is_file():
                failures.append(
                    f"{item_directory.relative_to(ROOT)}: missing required file {relative_file}"
                )

    if failures:
        raise SystemExit("Repository validation failed:\n" + "\n".join(failures))
    print(
        f"Validated {len(python_paths)} Python files, {len(json_paths)} JSON files, "
        f"and {len(seen_items)} Fabric item folders"
    )


if __name__ == "__main__":
    main()