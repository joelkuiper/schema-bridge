from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from .mapping import MappingConfig
from .resources import load_yaml, resolve_resource_path
from .shacl import ShaclConfig


@dataclass
class ProfileConfig:
    name: str
    kind: str | None = None
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


@dataclass
class IngestProfileConfig:
    name: str
    kind: str | None = None
    shacl: ShaclConfig | None = None
    validate: bool = True
    select_query: str | None = None
    table: str | None = None
    mode: str | None = None
    id_prefix: str | None = None
    batch_size: int | None = None
    base_url: str | None = None
    schema: str | None = None
    token: str | None = None
    graphql_mutation: str | None = None
    base_dir: Path | None = None


def _resolve_profile_file(name_or_path: str) -> Path:
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
    return profile_path


def _load_profile_data(name_or_path: str, expected_kind: str | None) -> tuple[dict, Path]:
    profile_path = _resolve_profile_file(name_or_path)
    profile_path_str = str(profile_path)
    profile_data = load_yaml(profile_path_str, "schema_bridge.resources")
    if Path(profile_path_str).exists():
        base_dir = Path(profile_path_str).parent
    else:
        resolved_profile_path = resolve_resource_path(profile_path_str, "schema_bridge.resources")
        base_dir = Path(resolved_profile_path).parent
    raw_kind = profile_data.get("kind")
    kind = str(raw_kind).lower() if raw_kind else None
    if expected_kind:
        if not kind:
            raise ValueError(f"Profile kind is required for {name_or_path} (expected {expected_kind})")
        if kind != expected_kind:
            raise ValueError(
                f"Profile kind mismatch for {name_or_path}: expected {expected_kind}, got {kind}"
            )
    return profile_data, base_dir


def load_profile(name_or_path: str, *, expected_kind: str | None = None) -> ProfileConfig:
    profile_data, base_dir = _load_profile_data(name_or_path, expected_kind)
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
        kind=str(profile_data.get("kind")).lower() if profile_data.get("kind") else None,
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


def _coerce_shacl(data: dict | None) -> ShaclConfig | None:
    if not data:
        return None
    if isinstance(data, dict) and data.get("shapes"):
        return ShaclConfig(
            shapes=str(data["shapes"]),
            validate=bool(data.get("validate", True)),
        )
    if isinstance(data, dict) and data.get("shacl"):
        return ShaclConfig(shapes=str(data["shacl"]), validate=bool(data.get("validate", True)))
    return None


def load_ingest_profile(name_or_path: str) -> IngestProfileConfig:
    profile_data, base_dir = _load_profile_data(name_or_path, expected_kind="ingest")

    validate_block = profile_data.get("validate") if isinstance(profile_data.get("validate"), dict) else {}
    extract_block = profile_data.get("extract") if isinstance(profile_data.get("extract"), dict) else {}
    upload_block = profile_data.get("upload") if isinstance(profile_data.get("upload"), dict) else {}
    graphql_block = profile_data.get("graphql") if isinstance(profile_data.get("graphql"), dict) else {}

    shacl = _coerce_shacl(profile_data.get("shacl") if "shacl" in profile_data else validate_block)
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
    validate_enabled = bool(validate_block.get("enabled", validate_block.get("validate", True)))

    select_query = (
        profile_data.get("select_query")
        or extract_block.get("sparql")
        or extract_block.get("select_query")
    )
    graphql_mutation = upload_block.get("graphql_mutation") or profile_data.get("graphql_mutation")

    return IngestProfileConfig(
        name=str(profile_data.get("name", name_or_path)),
        kind=str(profile_data.get("kind")).lower() if profile_data.get("kind") else None,
        shacl=shacl,
        validate=validate_enabled,
        select_query=str(select_query) if select_query else None,
        table=upload_block.get("table") or profile_data.get("table"),
        mode=upload_block.get("mode") or profile_data.get("mode"),
        id_prefix=upload_block.get("id_prefix") or profile_data.get("id_prefix"),
        batch_size=upload_block.get("batch_size") or profile_data.get("batch_size"),
        base_url=graphql_block.get("base_url") or profile_data.get("base_url"),
        schema=graphql_block.get("schema") or profile_data.get("schema"),
        token=graphql_block.get("token") or profile_data.get("token"),
        graphql_mutation=str(graphql_mutation) if graphql_mutation else None,
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
    profile = load_profile(profile_name, expected_kind="export")
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
