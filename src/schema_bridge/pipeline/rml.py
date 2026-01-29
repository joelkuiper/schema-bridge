from __future__ import annotations

from pathlib import Path

import morph_kgc
from rdflib import Graph

from .graphql import extract_rows, load_graphql_file
from .mapping import MappingConfig, load_raw_from_rows
from .profiles import ProfileConfig, resolve_profile_path


def materialize_rml(mapping_path: str, source_path: str | None = None) -> Graph:
    config = [
        "[DataSource1]",
        f"mappings: {mapping_path}",
    ]
    if source_path:
        config.append(f"file_path: {source_path}")
    return morph_kgc.materialize("\n".join(config))


def _materialize_graph(
    *,
    profile: ProfileConfig,
    from_format: str,
    input_path: Path,
    mapping_override: MappingConfig | None,
    root_key: str,
    rml_mapping: str | None,
    rml_source: str | None,
) -> Graph:
    if from_format == "rml" or profile.mapping_format == "rml":
        mapping_path = rml_mapping or profile.rml_mapping or str(input_path)
        if not mapping_path:
            raise ValueError("RML mapping path is required for RML conversion")
        resolved_mapping = resolve_profile_path(profile, mapping_path, "schema_bridge.resources")
        resolved_source = None
        if rml_source or profile.rml_source:
            resolved_source = resolve_profile_path(
                profile,
                rml_source or profile.rml_source,
                "schema_bridge.resources",
            )
        return materialize_rml(resolved_mapping, resolved_source)

    graphql_data = load_graphql_file(input_path)
    rows = extract_rows(graphql_data, root_key)
    raw = Graph()
    load_raw_from_rows(rows, raw, mapping_override or profile.mapping)
    return raw
