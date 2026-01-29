from __future__ import annotations

from pathlib import Path
import os
import typer

from rdflib import Graph

from schema_bridge.pipeline import (
    export_and_validate,
    fetch_graphql,
    load_text,
    extract_rows,
    load_profile,
    load_raw_from_rows,
    resolve_export,
    _materialize_graph,
    write_json,
    PaginationConfig,
)

app = typer.Typer(help="Schema Bridge CLI (GraphQL -> RDF canonical graph -> exports)")


@app.command()
def fetch(
    out: Path = typer.Option(..., "--out", "-o", help="Output JSON path"),
    base_url: str = typer.Option(
        os.getenv("SCHEMA_BRIDGE_BASE_URL", "https://emx2.dev.molgenis.org/"),
        help="Base URL for the EMX2 server",
    ),
    schema: str = typer.Option(
        os.getenv("SCHEMA_BRIDGE_SCHEMA", "catalogue-demo"),
        help="Schema name for the catalogue",
    ),
    profile: str = typer.Option(
        os.getenv("SCHEMA_BRIDGE_PROFILE", "dcat"),
        help="Profile name or YAML path",
    ),
    query: str | None = typer.Option(
        None,
        help="Query file under resources/graphql/ (overrides profile)",
    ),
    limit: int = typer.Option(
        int(os.getenv("SCHEMA_BRIDGE_LIMIT", "5")),
        help="Maximum number of resources to fetch (0 for all)",
    ),
    page_size: int = typer.Option(
        int(os.getenv("SCHEMA_BRIDGE_PAGE_SIZE", "200")),
        help="Rows per page for GraphQL paging",
    ),
    updated_since: str | None = typer.Option(
        None,
        help="Only fetch rows updated on/after this timestamp (ISO 8601)",
    ),
    updated_until: str | None = typer.Option(
        None,
        help="Only fetch rows updated before this timestamp (ISO 8601)",
    ),
) -> None:
    profile_cfg = load_profile(profile)
    pagination = PaginationConfig(page_size=page_size, max_rows=None if limit <= 0 else limit)
    query_path = query or profile_cfg.graphql_query or "graphql/resources.graphql"
    query_text = load_text(
        query_path if "/" in query_path else f"graphql/{query_path}",
        "schema_bridge.resources",
    )
    data = fetch_graphql(
        base_url,
        schema,
        query_text,
        root_key=profile_cfg.root_key,
        pagination=pagination,
        updated_since=updated_since,
        updated_until=updated_until,
    )
    write_json(data, out)


@app.command()
def convert(
    input_path: Path = typer.Argument(..., help="Input GraphQL JSON or RML mapping file"),
    out_dir: Path | None = typer.Option(
        None,
        "--out-dir",
        "-o",
        help="Output directory (stdout when omitted)",
    ),
    profile: str = typer.Option(
        os.getenv("SCHEMA_BRIDGE_PROFILE", "dcat"),
        help="Profile name or YAML path",
    ),
    mapping: Path | None = typer.Option(None, help="YAML mapping override path"),
    from_format: str = typer.Option(
        "graphql",
        "--from",
        help="Input format",
        case_sensitive=False,
    ),
    to_formats: str | None = typer.Option(
        None,
        "--to",
        "-t",
        help="Comma-separated output formats (overrides profile)",
    ),
    root_key: str | None = typer.Option(
        None,
        help="GraphQL data root (overrides profile)",
    ),
    select: str | None = typer.Option(
        None,
        help="SPARQL SELECT query file under resources/sparql/ (overrides profile)",
    ),
    construct: str | None = typer.Option(
        None,
        help="SPARQL CONSTRUCT query file under resources/sparql/ (overrides profile)",
    ),
    rml_mapping: str | None = typer.Option(
        None,
        help="RML mapping file path (used when --from rml or profile mapping_format=rml)",
    ),
    rml_source: str | None = typer.Option(
        None,
        help="Optional data source path to override rml:source (used with RML mappings)",
    ),
    validate: bool | None = typer.Option(
        None,
        "--validate/--no-validate",
        help="Enable/disable SHACL validation (overrides profile)",
    ),
    shacl_report: Path | None = typer.Option(
        None,
        help="Optional path to write SHACL validation report (TTL)",
    ),
) -> None:
    export = resolve_export(
        profile_name=profile,
        mapping_override=mapping,
        root_key=root_key,
        select_query=select,
        construct_query=construct,
        targets_override=to_formats,
        validate_override=validate,
    )

    raw_graph = _materialize_graph(
        profile=export.profile,
        from_format=from_format.lower(),
        input_path=input_path,
        mapping_override=export.mapping,
        root_key=export.root_key,
        rml_mapping=rml_mapping,
        rml_source=rml_source,
    )
    export_and_validate(raw_graph, export, out_dir, shacl_report, emit=typer.echo)


@app.command()
def run(
    out_dir: Path = typer.Option(Path("out"), "--out-dir", "-o", help="Output directory"),
    base_url: str = typer.Option(
        os.getenv("SCHEMA_BRIDGE_BASE_URL", "https://emx2.dev.molgenis.org/"),
        help="Base URL for the EMX2 server",
    ),
    schema: str = typer.Option(
        os.getenv("SCHEMA_BRIDGE_SCHEMA", "catalogue-demo"),
        help="Schema name for the catalogue",
    ),
    profile: str = typer.Option(
        os.getenv("SCHEMA_BRIDGE_PROFILE", "dcat"),
        help="Profile name or YAML path",
    ),
    query: str | None = typer.Option(
        None,
        help="Query file under resources/graphql/ (overrides profile)",
    ),
    root_key: str | None = typer.Option(
        None,
        help="GraphQL data root (overrides profile)",
    ),
    select: str | None = typer.Option(
        None,
        help="SPARQL SELECT query file under resources/sparql/ (overrides profile)",
    ),
    construct: str | None = typer.Option(
        None,
        help="SPARQL CONSTRUCT query file under resources/sparql/ (overrides profile)",
    ),
    mapping: Path | None = typer.Option(None, help="YAML mapping override path"),
    limit: int = typer.Option(
        int(os.getenv("SCHEMA_BRIDGE_LIMIT", "5")),
        help="Maximum number of resources to fetch (0 for all)",
    ),
    page_size: int = typer.Option(
        int(os.getenv("SCHEMA_BRIDGE_PAGE_SIZE", "200")),
        help="Rows per page for GraphQL paging",
    ),
    validate: bool | None = typer.Option(
        None,
        "--validate/--no-validate",
        help="Enable/disable SHACL validation (overrides profile)",
    ),
    shacl_report: Path | None = typer.Option(
        None,
        help="Optional path to write SHACL validation report (TTL)",
    ),
    updated_since: str | None = typer.Option(
        None,
        help="Only fetch rows updated on/after this timestamp (ISO 8601)",
    ),
    updated_until: str | None = typer.Option(
        None,
        help="Only fetch rows updated before this timestamp (ISO 8601)",
    ),
) -> None:
    profile_cfg = load_profile(profile)
    if profile_cfg.mapping_format == "rml":
        raise SystemExit("RML profiles are not supported with run; use convert instead.")
    export = resolve_export(
        profile_name=profile,
        mapping_override=mapping,
        root_key=root_key,
        select_query=select,
        construct_query=construct,
        targets_override=None,
        validate_override=validate,
    )
    pagination = PaginationConfig(page_size=page_size, max_rows=None if limit <= 0 else limit)
    query_path = query or export.profile.graphql_query or "graphql/resources.graphql"
    query_text = load_text(
        query_path if "/" in query_path else f"graphql/{query_path}",
        "schema_bridge.resources",
    )
    graphql_data = fetch_graphql(
        base_url,
        schema,
        query_text,
        root_key=export.root_key,
        pagination=pagination,
        updated_since=updated_since,
        updated_until=updated_until,
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    write_json(graphql_data, out_dir / "graphql.json")

    rows = extract_rows(graphql_data, export.root_key)
    raw_graph = Graph()
    load_raw_from_rows(rows, raw_graph, export.mapping)
    export_and_validate(raw_graph, export, out_dir, shacl_report, emit=typer.echo)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
