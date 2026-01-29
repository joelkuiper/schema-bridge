from __future__ import annotations

from pathlib import Path
import json
import os
import typer
import logging

from rdflib import Graph

from schema_bridge.logging import configure_logging
from schema_bridge.cli_helpers import resolve_graphql_target
from schema_bridge.pipeline import (
    export_and_validate,
    fetch_graphql,
    load_text,
    extract_rows,
    load_profile,
    load_ingest_profile,
    load_raw_from_rows,
    resolve_export,
    resolve_profile_path,
    _materialize_graph,
    write_json,
    PaginationConfig,
)
from schema_bridge.pipeline.ingest import (
    infer_rdf_format,
    load_rdf_graph,
    rows_from_rdf,
    validate_if_requested,
    graphql_post,
)

app = typer.Typer(help="Schema Bridge CLI (profile-driven export + ingest)")
logger = logging.getLogger("schema_bridge.cli")


@app.callback()
def _main(
    debug: bool = typer.Option(
        False,
        "--debug",
        help="Enable debug logging",
    )
) -> None:
    configure_logging(debug)


@app.command()
def fetch(
    out: Path = typer.Option(..., "--out", "-o", help="Output JSON path"),
    base_url: str | None = typer.Option(
        None,
        help="Base URL for the EMX2 server (overrides profile/environment)",
    ),
    schema: str | None = typer.Option(
        None,
        help="Schema name for the catalogue (overrides profile/environment)",
    ),
    endpoint: str | None = typer.Option(
        None,
        "--graphql-endpoint",
        help="Full GraphQL endpoint URL (overrides base_url/schema)",
    ),
    profile: str = typer.Option(
        os.getenv("SCHEMA_BRIDGE_PROFILE", "dcat"),
        "--profile",
        help="Profile name, folder, or YAML path",
    ),
    query: str | None = typer.Option(
        None,
        help="GraphQL query file path (overrides profile)",
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
    debug: bool = typer.Option(
        False,
        "--debug",
        help="Enable debug logging",
    ),
) -> None:
    configure_logging(debug)
    logger.debug("Starting fetch: base_url=%s schema=%s profile=%s endpoint=%s", base_url, schema, profile, endpoint)
    profile_cfg = load_profile(profile, expected_kind="export")
    resolved_endpoint, resolved_base_url, resolved_schema = resolve_graphql_target(
        profile=profile_cfg,
        base_url=base_url,
        schema=schema,
        endpoint=endpoint,
    )
    pagination = PaginationConfig(page_size=page_size, max_rows=None if limit <= 0 else limit)
    query_path = query or profile_cfg.graphql_query or "profiles/dcat/graphql/query.graphql"
    query_path = resolve_profile_path(profile_cfg, query_path, "schema_bridge.resources")
    query_text = load_text(query_path, "schema_bridge.resources")
    data = fetch_graphql(
        resolved_base_url,
        resolved_schema,
        query_text,
        root_key=profile_cfg.root_key,
        pagination=pagination,
        updated_since=updated_since,
        updated_until=updated_until,
        endpoint=resolved_endpoint,
    )
    write_json(data, out)
    logger.debug("Fetch complete: wrote %s", out)


@app.command()
def convert(
    input_path: Path = typer.Argument(..., help="Input GraphQL JSON or RML mapping file"),
    profile: str = typer.Option(
        os.getenv("SCHEMA_BRIDGE_PROFILE", "dcat"),
        "--profile",
        help="Profile name, folder, or YAML path",
    ),
    mapping: Path | None = typer.Option(None, help="YAML mapping override path"),
    from_format: str = typer.Option(
        "graphql",
        "--from",
        help="Input format",
        case_sensitive=False,
    ),
    output_format: str = typer.Option(
        ...,
        "--format",
        "-f",
        help="Output format: csv, json, jsonld, ttl, rdfxml, nt",
        case_sensitive=False,
    ),
    root_key: str | None = typer.Option(
        None,
        help="GraphQL data root (overrides profile)",
    ),
    select: str | None = typer.Option(
        None,
        help="SPARQL SELECT query file (overrides profile)",
    ),
    construct: str | None = typer.Option(
        None,
        help="SPARQL CONSTRUCT query file (overrides profile)",
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
    debug: bool = typer.Option(
        False,
        "--debug",
        help="Enable debug logging",
    ),
) -> None:
    configure_logging(debug)
    logger.debug("Starting convert: input=%s profile=%s format=%s", input_path, profile, output_format)
    export = resolve_export(
        profile_name=profile,
        mapping_override=mapping,
        root_key=root_key,
        select_query=select,
        construct_query=construct,
        target_format=output_format.lower(),
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
    export_and_validate(
        raw_graph,
        export,
        None,
        shacl_report,
        emit=lambda text: typer.echo(text, nl=False),
    )
    logger.debug("Convert complete")


@app.command()
def export(
    base_url: str | None = typer.Option(
        None,
        help="Base URL for the EMX2 server (overrides profile/environment)",
    ),
    schema: str | None = typer.Option(
        None,
        help="Schema name for the catalogue (overrides profile/environment)",
    ),
    endpoint: str | None = typer.Option(
        None,
        "--graphql-endpoint",
        help="Full GraphQL endpoint URL (overrides base_url/schema)",
    ),
    profile: str = typer.Option(
        os.getenv("SCHEMA_BRIDGE_PROFILE", "dcat"),
        "--profile",
        help="Profile name, folder, or YAML path",
    ),
    query: str | None = typer.Option(
        None,
        help="GraphQL query file path (overrides profile)",
    ),
    root_key: str | None = typer.Option(
        None,
        help="GraphQL data root (overrides profile)",
    ),
    select: str | None = typer.Option(
        None,
        help="SPARQL SELECT query file (overrides profile)",
    ),
    construct: str | None = typer.Option(
        None,
        help="SPARQL CONSTRUCT query file (overrides profile)",
    ),
    mapping: Path | None = typer.Option(None, help="YAML mapping override path"),
    output_format: str = typer.Option(
        ...,
        "--format",
        "-f",
        help="Output format: csv, json, jsonld, ttl, rdfxml, nt",
        case_sensitive=False,
    ),
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
    debug: bool = typer.Option(
        False,
        "--debug",
        help="Enable debug logging",
    ),
) -> None:
    configure_logging(debug)
    logger.debug(
        "Starting export: base_url=%s schema=%s profile=%s format=%s endpoint=%s",
        base_url,
        schema,
        profile,
        output_format,
        endpoint,
    )
    profile_cfg = load_profile(profile, expected_kind="export")
    if profile_cfg.mapping_format == "rml":
        raise SystemExit("RML profiles are not supported with export; use convert instead.")
    export = resolve_export(
        profile_name=profile,
        mapping_override=mapping,
        root_key=root_key,
        select_query=select,
        construct_query=construct,
        target_format=output_format.lower(),
        validate_override=validate,
    )
    pagination = PaginationConfig(page_size=page_size, max_rows=None if limit <= 0 else limit)
    resolved_endpoint, resolved_base_url, resolved_schema = resolve_graphql_target(
        profile=export.profile,
        base_url=base_url,
        schema=schema,
        endpoint=endpoint,
    )
    query_path = query or export.profile.graphql_query or "profiles/dcat/graphql/query.graphql"
    query_path = resolve_profile_path(export.profile, query_path, "schema_bridge.resources")
    query_text = load_text(query_path, "schema_bridge.resources")
    graphql_data = fetch_graphql(
        resolved_base_url,
        resolved_schema,
        query_text,
        root_key=export.root_key,
        pagination=pagination,
        updated_since=updated_since,
        updated_until=updated_until,
        endpoint=resolved_endpoint,
    )
    rows = extract_rows(graphql_data, export.root_key)
    raw_graph = Graph()
    load_raw_from_rows(rows, raw_graph, export.mapping)
    export_and_validate(
        raw_graph,
        export,
        None,
        shacl_report,
        emit=lambda text: typer.echo(text, nl=False),
    )
    logger.debug("Export complete")


@app.command()
def ingest(
    input_path: Path = typer.Argument(
        ..., help="Input RDF file (TTL, JSON-LD, RDF/XML, or N-Triples)"
    ),
    base_url: str | None = typer.Option(
        None,
        help="Base URL for the EMX2 server (overrides profile)",
    ),
    schema: str | None = typer.Option(
        None,
        help="Schema name for the catalogue (overrides profile)",
    ),
    profile: str = typer.Option(
        os.getenv("SCHEMA_BRIDGE_PROFILE", "ingest-dcat"),
        "--profile",
        help="Profile name, folder, or YAML path",
    ),
    table: str | None = typer.Option(None, help="Target EMX2 table name (overrides profile)"),
    mode: str | None = typer.Option(None, help="Mutation mode: upsert or insert (overrides profile)"),
    rdf_format: str | None = typer.Option(
        None,
        "--format",
        "-f",
        help="RDF format (turtle, json-ld, rdfxml, nt); inferred from file suffix when omitted",
    ),
    select: str | None = typer.Option(
        None,
        help="SPARQL SELECT query file (overrides profile)",
    ),
    mutation_file: str | None = typer.Option(
        None,
        help="GraphQL mutation file (overrides profile)",
    ),
    id_prefix: str | None = typer.Option(
        None,
        help="Prefix to use for generated EMX2 ids (overrides profile)",
    ),
    validate: bool | None = typer.Option(
        None,
        "--validate/--no-validate",
        help="Enable/disable SHACL validation (overrides profile, defaults to enabled)",
    ),
    batch_size: int | None = typer.Option(
        None,
        help="Rows per GraphQL mutation (overrides profile)",
    ),
    token: str | None = typer.Option(
        None,
        help="Bearer token for GraphQL auth (overrides profile)",
    ),
    dry_run: bool = typer.Option(False, help="Do not upload, print rows as JSON"),
    out: Path | None = typer.Option(
        None,
        "--out",
        "-o",
        help="Optional path to write the generated rows as JSON",
    ),
    debug: bool = typer.Option(
        False,
        "--debug",
        help="Enable debug logging",
    ),
) -> None:
    configure_logging(debug)
    logger.debug("Starting ingest: input=%s profile=%s", input_path, profile)
    profile_cfg = load_ingest_profile(profile)
    final_base_url = base_url or profile_cfg.base_url or os.getenv(
        "SCHEMA_BRIDGE_BASE_URL",
        "https://emx2.dev.molgenis.org/",
    )
    final_schema = schema or profile_cfg.schema or os.getenv(
        "SCHEMA_BRIDGE_SCHEMA",
        "catalogue-demo",
    )
    final_table = table or profile_cfg.table or "Resource"
    final_mode = (mode or profile_cfg.mode or "upsert").lower()
    final_id_prefix = id_prefix or profile_cfg.id_prefix or "import-"
    final_batch_size = batch_size or int(profile_cfg.batch_size or 100)
    final_token = token or profile_cfg.token or os.getenv("SCHEMA_BRIDGE_TOKEN")
    final_validate = validate if validate is not None else profile_cfg.validate
    final_mutation_file = mutation_file or profile_cfg.graphql_mutation

    resolved_format = infer_rdf_format(input_path, rdf_format)
    graph = load_rdf_graph(input_path, resolved_format)
    validate_if_requested(graph, profile_cfg, final_validate)

    rows = rows_from_rdf(
        graph,
        profile=profile_cfg,
        select_override=select,
        id_prefix=final_id_prefix,
    )
    logger.debug("Prepared %s row(s) for ingest", len(rows))
    if out:
        write_json({"rows": rows}, out)
    if dry_run:
        typer.echo(json.dumps({"rows": rows}, indent=2))
        return
    if not rows:
        typer.echo("No rows to upload")
        return

    if final_mode not in {"upsert", "insert"}:
        raise ValueError("Mode must be 'upsert' or 'insert'")

    if final_mutation_file:
        mutation_path = resolve_profile_path(profile_cfg, final_mutation_file, "schema_bridge.resources")
        query = Path(mutation_path).read_text(encoding="utf-8")
    else:
        query = f"mutation ingest($value:[{final_table}Input]){{{final_mode}({final_table}:$value){{message}}}}"

    for i in range(0, len(rows), final_batch_size):
        batch = rows[i : i + final_batch_size]
        payload = {"query": query, "variables": {"value": batch}}
        graphql_post(final_base_url, final_schema, payload, final_token)
    typer.echo(f"Uploaded {len(rows)} row(s) to {final_schema}.{final_table} via {final_mode}")
    logger.debug("Ingest complete")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
