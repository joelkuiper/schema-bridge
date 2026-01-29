from __future__ import annotations

from pathlib import Path
import json
import os
import uuid

import typer
from gql import Client, gql
from gql.transport.exceptions import TransportQueryError
from gql.transport.requests import RequestsHTTPTransport
from rdflib import Graph

from dataclasses import dataclass
import yaml

from schema_bridge.pipeline import ShaclConfig, load_text, validate_graph, write_json

app = typer.Typer(help="Schema Bridge ingest CLI (RDF -> GraphQL upsert)")


@dataclass
class IngestProfile:
    name: str
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


def _load_yaml(path: Path) -> dict:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Expected mapping in YAML file: {path}")
    return data


def _ingest_profile_path(name_or_path: str) -> Path:
    candidate = Path(name_or_path)
    if candidate.exists():
        return candidate
    if not candidate.suffix:
        candidate = candidate.with_suffix(".yml")
    resource_candidate = Path(__file__).parent / "resources" / "ingest_profiles" / candidate.name
    if resource_candidate.exists():
        return resource_candidate
    raise FileNotFoundError(f"Ingest profile not found: {name_or_path}")


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


def load_ingest_profile(name_or_path: str) -> IngestProfile:
    profile_path = _ingest_profile_path(name_or_path)
    data = _load_yaml(profile_path)
    base_dir = profile_path.parent

    validate_block = data.get("validate") if isinstance(data.get("validate"), dict) else {}
    extract_block = data.get("extract") if isinstance(data.get("extract"), dict) else {}
    upload_block = data.get("upload") if isinstance(data.get("upload"), dict) else {}
    graphql_block = data.get("graphql") if isinstance(data.get("graphql"), dict) else {}

    shacl = _coerce_shacl(data.get("shacl") if "shacl" in data else validate_block)
    validate_enabled = bool(validate_block.get("enabled", validate_block.get("validate", True)))

    select_query = (
        data.get("select_query")
        or extract_block.get("sparql")
        or extract_block.get("select_query")
    )
    graphql_mutation = upload_block.get("graphql_mutation") or data.get("graphql_mutation")

    return IngestProfile(
        name=str(data.get("name", name_or_path)),
        shacl=shacl,
        validate=validate_enabled,
        select_query=str(select_query) if select_query else None,
        table=upload_block.get("table") or data.get("table"),
        mode=upload_block.get("mode") or data.get("mode"),
        id_prefix=upload_block.get("id_prefix") or data.get("id_prefix"),
        batch_size=upload_block.get("batch_size") or data.get("batch_size"),
        base_url=graphql_block.get("base_url") or data.get("base_url"),
        schema=graphql_block.get("schema") or data.get("schema"),
        token=graphql_block.get("token") or data.get("token"),
        graphql_mutation=str(graphql_mutation) if graphql_mutation else None,
        base_dir=base_dir,
    )


def resolve_ingest_path(profile: IngestProfile, path: str) -> str:
    candidate = Path(path)
    if candidate.is_absolute() or candidate.exists():
        return str(candidate.resolve())
    if profile.base_dir:
        profile_candidate = profile.base_dir / path
        if profile_candidate.exists():
            return str(profile_candidate.resolve())
    return str((Path(__file__).parent / "resources" / path).resolve())


def _normalize_rdf_format(value: str) -> str:
    normalized = value.strip().lower()
    aliases = {
        "turtle": "turtle",
        "ttl": "turtle",
        "json-ld": "json-ld",
        "jsonld": "json-ld",
        "rdfxml": "xml",
        "rdf/xml": "xml",
        "xml": "xml",
        "rdf": "xml",
        "ntriples": "nt",
        "n-triples": "nt",
        "nt": "nt",
    }
    return aliases.get(normalized, normalized)


def _infer_format(path: Path, explicit: str | None) -> str:
    if explicit:
        return _normalize_rdf_format(explicit)
    suffix = path.suffix.lower()
    if suffix in {".ttl", ".turtle"}:
        return "turtle"
    if suffix in {".jsonld", ".json"}:
        return "json-ld"
    if suffix in {".rdf", ".xml"}:
        return "xml"
    if suffix in {".nt"}:
        return "nt"
    raise ValueError("Unable to infer RDF format; use --format")


def _load_rdf_graph(path: Path, rdf_format: str) -> Graph:
    graph = Graph()
    graph.parse(path.as_posix(), format=rdf_format)
    return graph


def _select_rows(graph: Graph, query_path: str) -> list[dict]:
    query = load_text(query_path, "schema_bridge.resources")
    rows: list[dict] = []
    for row in graph.query(query):
        rows.append({k: str(v) if v is not None else "" for k, v in row.asdict().items()})
    return rows


def _sanitize_email(value: str) -> str:
    if value.startswith("mailto:"):
        return value[len("mailto:") :]
    return value


def _rows_from_rdf(
    graph: Graph,
    *,
    profile: IngestProfile,
    select_override: str | None,
    id_prefix: str,
) -> list[dict]:
    select_query = select_override or profile.select_query
    if not select_query:
        raise ValueError("Ingest requires an ingest_select_query (set in profile or via --select)")
    resolved_select = resolve_ingest_path(profile, select_query)
    raw_rows = _select_rows(graph, resolved_select)
    rows: list[dict] = []
    for raw in raw_rows:
        name = raw.get("name", "").strip()
        if not name:
            continue
        row: dict[str, str] = {"id": f"{id_prefix}{uuid.uuid4().hex}", "name": name}
        description = raw.get("description", "").strip()
        if description:
            row["description"] = description
        website = raw.get("website", "").strip()
        if website:
            row["website"] = website
        contact = raw.get("contactEmail", "").strip()
        if contact:
            row["contactEmail"] = _sanitize_email(contact)
        rows.append(row)
    return rows


def _validate_if_requested(
    graph: Graph,
    profile: IngestProfile,
    validate: bool,
) -> None:
    shacl = profile.shacl
    if not validate or not shacl:
        return
    conforms, report = validate_graph(graph, ShaclConfig(shapes=shacl.shapes, validate=True))
    if not conforms:
        report_text = report.serialize(format="turtle")
        raise SystemExit(f"SHACL validation failed:\n{report_text}")


def _graphql_post(
    base_url: str,
    schema: str,
    payload: dict,
    token: str | None,
) -> dict:
    endpoint = f"{base_url.rstrip('/')}/{schema}/graphql"
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    transport = RequestsHTTPTransport(url=endpoint, headers=headers, timeout=30)
    client = Client(transport=transport, fetch_schema_from_transport=False)
    try:
        result = client.execute(
            gql(payload["query"]),
            variable_values=payload.get("variables") or {},
        )
    except TransportQueryError as exc:
        raise RuntimeError(f"GraphQL errors: {exc.errors}") from exc
    return {"data": result}


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
        help="Ingest profile name or YAML path",
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
) -> None:
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

    resolved_format = _infer_format(input_path, rdf_format)
    graph = _load_rdf_graph(input_path, resolved_format)
    _validate_if_requested(graph, profile_cfg, final_validate)

    rows = _rows_from_rdf(
        graph,
        profile=profile_cfg,
        select_override=select,
        id_prefix=final_id_prefix,
    )
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
        mutation_path = resolve_ingest_path(profile_cfg, final_mutation_file)
        query = Path(mutation_path).read_text(encoding="utf-8")
    else:
        query = f"mutation ingest($value:[{final_table}Input]){{{final_mode}({final_table}:$value){{message}}}}"

    for i in range(0, len(rows), final_batch_size):
        batch = rows[i : i + final_batch_size]
        payload = {"query": query, "variables": {"value": batch}}
        _graphql_post(final_base_url, final_schema, payload, final_token)
    typer.echo(f"Uploaded {len(rows)} row(s) to {final_schema}.{final_table} via {final_mode}")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
