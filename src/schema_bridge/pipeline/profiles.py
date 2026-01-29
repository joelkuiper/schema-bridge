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
    root_key: str = "Resources"
    select_query: str | None = None
    construct_query: str | None = None
    ingest_select_query: str | None = None
    outputs: list[str] = field(default_factory=lambda: ["csv", "json", "jsonld", "ttl"])
    mapping: MappingConfig = field(default_factory=MappingConfig)
    shacl: ShaclConfig | None = None
    mapping_format: str = "raw"
    rml_mapping: str | None = None
    rml_source: str | None = None
    base_dir: Path | None = None


def load_profile(name_or_path: str) -> ProfileConfig:
    profile_path = name_or_path
    if not Path(profile_path).exists() and not profile_path.endswith((".yml", ".yaml")):
        profile_path = f"{profile_path}.yml"
    if not Path(profile_path).exists() and not profile_path.startswith("profiles/"):
        profile_path = f"profiles/{profile_path}"
    profile_data = load_yaml(profile_path, "schema_bridge.resources")
    base_dir = Path(profile_path).parent if Path(profile_path).exists() else None
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
    return ProfileConfig(
        name=str(profile_data.get("name", name_or_path)),
        graphql_query=profile_data.get("graphql_query") or fetch_data.get("graphql"),
        root_key=str(profile_data.get("root_key", fetch_data.get("root_key", "Resources"))),
        select_query=profile_data.get("select_query") or export_data.get("select"),
        construct_query=profile_data.get("construct_query") or export_data.get("construct"),
        ingest_select_query=profile_data.get("ingest_select_query") or export_data.get("ingest_select"),
        outputs=list(profile_data.get("outputs", export_data.get("outputs", ["csv", "json", "jsonld", "ttl"]))),
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


def _final_targets(profile: ProfileConfig, override: str | None) -> list[str]:
    if override:
        return [t.strip() for t in override.split(",") if t.strip()]
    return profile.outputs


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
    targets_override: str | None,
    validate_override: bool | None,
) -> ResolvedExport:
    profile = load_profile(profile_name)
    mapping = load_mapping_override(str(mapping_override)) if mapping_override else None
    return ResolvedExport(
        profile=profile,
        mapping=mapping or profile.mapping,
        root_key=root_key or profile.root_key,
        select_query=select_query or profile.select_query,
        construct_query=construct_query or profile.construct_query,
        targets=_final_targets(profile, targets_override),
        validate=_final_validate(profile, validate_override),
    )
