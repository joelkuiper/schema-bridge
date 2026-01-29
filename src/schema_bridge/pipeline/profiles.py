from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from .mapping import MappingConfig
from .resources import load_yaml, resolve_resource_path
from .shacl import ShaclConfig


@dataclass
class ProfileConfig:
    name: str
    graphql_query: str | None = None
    graphql_endpoint: str | None = None
    base_url: str | None = None
    schema: str | None = None
    root_key: str = "Resources"
    select_query: str | None = None
    construct_query: str | None = None
    ingest_select_query: str | None = None
    mapping: MappingConfig = field(default_factory=MappingConfig)
    shacl: ShaclConfig | None = None
    mapping_format: str = "raw"
    rml_mapping: str | None = None
    rml_source: str | None = None
    base_dir: Path | None = None


def load_profile(name_or_path: str) -> ProfileConfig:
    profile_path = Path(name_or_path)
    if profile_path.exists():
        if profile_path.is_dir():
            profile_path = profile_path / "profile.yml"
    else:
        if name_or_path.endswith((".yml", ".yaml")):
            profile_path = Path(name_or_path)
        else:
            candidate_dir = Path(f"{name_or_path}/profile.yml")
            candidate_file = Path(f"{name_or_path}.yml")
            if candidate_dir.exists():
                profile_path = candidate_dir
            elif candidate_file.exists():
                profile_path = candidate_file
            elif name_or_path.startswith("profiles/"):
                profile_path = Path(name_or_path) / "profile.yml"
            else:
                profile_path = Path("profiles") / name_or_path / "profile.yml"
    profile_path_str = str(profile_path)
    profile_data = load_yaml(profile_path_str, "schema_bridge.resources")
    if Path(profile_path_str).exists():
        base_dir = Path(profile_path_str).parent
    else:
        resolved_profile_path = resolve_resource_path(profile_path_str, "schema_bridge.resources")
        base_dir = Path(resolved_profile_path).parent
    fetch_data = profile_data.get("fetch") if isinstance(profile_data.get("fetch"), dict) else {}
    export_data = profile_data.get("export") if isinstance(profile_data.get("export"), dict) else {}
    validate_data = profile_data.get("validate") if isinstance(profile_data.get("validate"), dict) else {}
    mapping = MappingConfig.from_dict(profile_data.get("mapping"))  # type: ignore[arg-type]
    shacl_data = profile_data.get("shacl") or validate_data
    shacl = None
    if isinstance(shacl_data, dict) and shacl_data.get("shapes"):
        shacl = ShaclConfig(
            shapes=str(shacl_data["shapes"]),
            validate=bool(shacl_data.get("validate", True)),
        )
    if isinstance(shacl_data, dict) and shacl_data.get("shacl"):
        shacl = ShaclConfig(
            shapes=str(shacl_data["shacl"]),
            validate=bool(shacl_data.get("enabled", shacl_data.get("validate", True))),
        )
    if shacl and shacl.shapes:
        candidate = Path(shacl.shapes)
        if candidate.is_absolute() or candidate.exists():
            resolved_shapes = str(candidate.resolve())
        else:
            resolved_shapes = resolve_resource_path(
                str((base_dir / shacl.shapes) if base_dir else shacl.shapes),
                "schema_bridge.resources",
            )
        shacl = ShaclConfig(shapes=resolved_shapes, validate=shacl.validate)
    graphql_fallbacks = list(
        profile_data.get("graphql_fallbacks", fetch_data.get("graphql_fallbacks", []))
    )
    graphql_query = (
        graphql_fallbacks[0]
        if graphql_fallbacks
        else profile_data.get("graphql_query") or fetch_data.get("graphql")
    )
    return ProfileConfig(
        name=str(profile_data.get("name", name_or_path)),
        graphql_query=graphql_query,
        graphql_endpoint=fetch_data.get("endpoint") or profile_data.get("graphql_endpoint"),
        base_url=fetch_data.get("base_url") or profile_data.get("base_url"),
        schema=fetch_data.get("schema") or profile_data.get("schema"),
        root_key=str(profile_data.get("root_key", fetch_data.get("root_key", "Resources"))),
        select_query=profile_data.get("select_query") or export_data.get("select"),
        construct_query=profile_data.get("construct_query") or export_data.get("construct"),
        ingest_select_query=profile_data.get("ingest_select_query") or export_data.get("ingest_select"),
        mapping=mapping,
        shacl=shacl,
        mapping_format=str(profile_data.get("mapping_format", "raw")).lower(),
        rml_mapping=profile_data.get("rml_mapping"),
        rml_source=profile_data.get("rml_source"),
        base_dir=base_dir,
    )


def load_mapping_override(path: str | None) -> MappingConfig | None:
    if not path:
        return None
    data = load_yaml(path, "schema_bridge.resources")
    return MappingConfig.from_dict(data.get("mapping", data))


def resolve_profile_path(profile: ProfileConfig, path: str, package: str) -> str:
    candidate = Path(path)
    if candidate.is_absolute() or candidate.exists():
        return str(candidate.resolve())
    if profile.base_dir:
        profile_candidate = profile.base_dir / path
        if profile_candidate.exists():
            return str(profile_candidate.resolve())
    return resolve_resource_path(path, package)


@dataclass
class ResolvedExport:
    profile: ProfileConfig
    mapping: MappingConfig
    root_key: str
    select_query: str | None
    construct_query: str | None
    targets: list[str]
    validate: bool


def _final_validate(profile: ProfileConfig, override: bool | None) -> bool:
    if override is not None:
        return override
    return profile.shacl.validate if profile.shacl else False


def resolve_export(
    *,
    profile_name: str,
    mapping_override: Path | None,
    root_key: str | None,
    select_query: str | None,
    construct_query: str | None,
    target_format: str | None,
    validate_override: bool | None,
) -> ResolvedExport:
    profile = load_profile(profile_name)
    mapping = load_mapping_override(str(mapping_override)) if mapping_override else None
    if not target_format:
        raise ValueError("Output format is required")
    normalized_format = target_format.strip().lower()
    if not normalized_format:
        raise ValueError("Output format is required")
    resolved_select = None
    if select_query or profile.select_query:
        resolved_select = resolve_profile_path(
            profile,
            select_query or profile.select_query,
            "schema_bridge.resources",
        )
    resolved_construct = None
    if construct_query or profile.construct_query:
        resolved_construct = resolve_profile_path(
            profile,
            construct_query or profile.construct_query,
            "schema_bridge.resources",
        )
    return ResolvedExport(
        profile=profile,
        mapping=mapping or profile.mapping,
        root_key=root_key or profile.root_key,
        select_query=resolved_select,
        construct_query=resolved_construct,
        targets=[normalized_format],
        validate=_final_validate(profile, validate_override),
    )
