from __future__ import annotations

from pathlib import Path
import uuid

from gql import Client, gql
from gql.transport.exceptions import TransportQueryError
from gql.transport.requests import RequestsHTTPTransport
from rdflib import Graph

import logging

from .profiles import IngestProfileConfig
from .shacl import ShaclConfig, validate_graph
from .profiles import resolve_profile_path
from .sparql import select_rows as sparql_select_rows

logger = logging.getLogger("schema_bridge.pipeline.ingest")


def normalize_rdf_format(value: str) -> str:
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


def infer_rdf_format(path: Path, explicit: str | None) -> str:
    if explicit:
        return normalize_rdf_format(explicit)
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


def load_rdf_graph(path: Path, rdf_format: str) -> Graph:
    logger.debug("Loading RDF graph: %s (format=%s)", path, rdf_format)
    graph = Graph()
    graph.parse(path.as_posix(), format=rdf_format)
    return graph


def sanitize_email(value: str) -> str:
    if value.startswith("mailto:"):
        return value[len("mailto:") :]
    return value


def rows_from_rdf(
    graph: Graph,
    *,
    profile: IngestProfileConfig,
    select_override: str | None,
    id_prefix: str,
) -> list[dict]:
    select_query = select_override or profile.select_query
    if not select_query:
        raise ValueError("Ingest requires a select query (set in profile or via --select)")
    resolved_select = resolve_profile_path(profile, select_query, "schema_bridge.resources")
    raw_rows = sparql_select_rows(graph, resolved_select)
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
            row["contactEmail"] = sanitize_email(contact)
        rows.append(row)
    return rows


def validate_if_requested(
    graph: Graph,
    profile: IngestProfileConfig,
    validate: bool,
) -> None:
    shacl = profile.shacl
    if not validate or not shacl:
        return
    logger.debug("Validating ingest graph with SHACL: %s", shacl.shapes)
    conforms, report = validate_graph(graph, ShaclConfig(shapes=shacl.shapes, validate=True))
    if not conforms:
        report_text = report.serialize(format="turtle")
        raise SystemExit(f"SHACL validation failed:\n{report_text}")


def graphql_post(
    base_url: str,
    schema: str,
    payload: dict,
    token: str | None,
) -> dict:
    logger.debug("Posting GraphQL to %s/%s/graphql", base_url.rstrip("/"), schema)
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
