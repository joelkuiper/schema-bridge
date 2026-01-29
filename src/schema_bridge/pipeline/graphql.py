from __future__ import annotations

from pathlib import Path
import json
import os

from gql import Client, gql
from gql.transport.exceptions import TransportQueryError
from gql.transport.requests import RequestsHTTPTransport


def load_graphql_file(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def fetch_graphql(base_url: str, schema: str, query: str, variables: dict | None = None) -> dict:
    fixture = os.getenv("SCHEMA_BRIDGE_GRAPHQL_FIXTURE")
    if fixture:
        return load_graphql_file(Path(fixture))
    endpoint = f"{base_url.rstrip('/')}/{schema}/graphql"
    transport = RequestsHTTPTransport(url=endpoint, timeout=30)
    client = Client(transport=transport, fetch_schema_from_transport=False)
    try:
        result = client.execute(gql(query), variable_values=variables or {})
    except TransportQueryError as exc:
        raise RuntimeError(f"GraphQL errors: {exc.errors}") from exc
    return {"data": result}


def extract_rows(graphql_data: dict, root_key: str) -> list[dict]:
    data = graphql_data.get("data", {})
    if root_key not in data:
        raise KeyError(f"Missing data root '{root_key}' in GraphQL response")
    return data[root_key] or []
