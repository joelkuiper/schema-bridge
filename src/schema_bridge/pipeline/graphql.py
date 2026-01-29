from __future__ import annotations

from dataclasses import dataclass
from typing import Callable
from datetime import datetime, timezone
from pathlib import Path
import json
import os

from gql import Client, gql
from gql.transport.exceptions import TransportQueryError
from gql.transport.requests import RequestsHTTPTransport
import logging

logger = logging.getLogger("schema_bridge.pipeline.graphql")


@dataclass(frozen=True)
class PaginationConfig:
    page_size: int
    max_rows: int | None = None
    offset: int = 0


def load_graphql_file(path: Path) -> dict:
    logger.debug("Loading GraphQL fixture: %s", path)
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _build_updated_filter(
    updated_since: str | None, updated_until: str | None
) -> dict | None:
    if not updated_since and not updated_until:
        return None
    start = updated_since or "0001-01-01T00:00:00Z"
    end = updated_until
    if not end:
        now = datetime.now(timezone.utc).replace(microsecond=0)
        end = now.isoformat().replace("+00:00", "Z")
    logger.debug("Using updated filter: %s..%s", start, end)
    return {"between": {"mg_updatedOn": [start, end]}}


def _merge_filters(existing: dict | None, updated_filter: dict | None) -> dict | None:
    if not updated_filter:
        return existing
    if not existing:
        return updated_filter
    return {"and": [existing, updated_filter]}


def _execute_graphql(client: Client, query: str, variables: dict | None = None) -> dict:
    try:
        result = client.execute(gql(query), variable_values=variables or {})
    except TransportQueryError as exc:
        raise RuntimeError(f"GraphQL errors: {exc.errors}") from exc
    return result


def _paginate_graphql(
    *,
    execute: Callable[[str, dict | None], dict],
    query: str,
    variables: dict | None,
    root_key: str,
    pagination: PaginationConfig,
    updated_filter: dict | None,
) -> dict:
    logger.debug(
        "Paginating GraphQL results: root_key=%s page_size=%s max_rows=%s",
        root_key,
        pagination.page_size,
        pagination.max_rows,
    )
    rows: list[dict] = []
    total = 0
    offset = pagination.offset
    while True:
        page_limit = pagination.page_size
        if pagination.max_rows is not None:
            remaining = pagination.max_rows - total
            if remaining <= 0:
                break
            page_limit = min(page_limit, remaining)
        page_vars = dict(variables or {})
        page_vars["limit"] = page_limit
        page_vars["offset"] = offset
        if updated_filter is not None or "filter" in page_vars:
            page_vars["filter"] = _merge_filters(
                page_vars.get("filter"), updated_filter
            )
        result = execute(query, page_vars)
        data = result.get(root_key)
        if not isinstance(data, list):
            raise RuntimeError(
                f"Expected list for '{root_key}', got {type(data).__name__}"
            )
        rows.extend(data)
        total += len(data)
        logger.debug("Fetched %s rows (total=%s)", len(data), total)
        if len(data) < page_limit:
            break
        offset += len(data)
    return {"data": {root_key: rows}}


def fetch_graphql(
    base_url: str | None,
    schema: str | None,
    query: str,
    variables: dict | None = None,
    *,
    root_key: str | None = None,
    pagination: PaginationConfig | None = None,
    updated_since: str | None = None,
    updated_until: str | None = None,
    endpoint: str | None = None,
) -> dict:
    fixture = os.getenv("SCHEMA_BRIDGE_GRAPHQL_FIXTURE")
    if fixture:
        logger.debug("Using GraphQL fixture from SCHEMA_BRIDGE_GRAPHQL_FIXTURE")
        return load_graphql_file(Path(fixture))
    if endpoint:
        url = endpoint
    else:
        if not base_url or not schema:
            raise ValueError(
                "GraphQL base_url and schema are required when endpoint is not set"
            )
        url = f"{base_url.rstrip('/')}/{schema}/graphql"
    logger.debug("Fetching GraphQL from %s (root_key=%s)", url, root_key)
    transport = RequestsHTTPTransport(url=url, timeout=30)
    client = Client(transport=transport, fetch_schema_from_transport=False)
    updated_filter = _build_updated_filter(updated_since, updated_until)
    if pagination:
        if not root_key:
            raise ValueError("Pagination requires a root_key to merge results")
        return _paginate_graphql(
            execute=lambda q, v: _execute_graphql(client, q, v),
            query=query,
            variables=variables,
            root_key=root_key,
            pagination=pagination,
            updated_filter=updated_filter,
        )
    merged_vars = dict(variables or {})
    if updated_filter is not None:
        merged_vars["filter"] = _merge_filters(
            merged_vars.get("filter"), updated_filter
        )
    logger.debug("Executing GraphQL query (no pagination)")
    result = _execute_graphql(client, query, merged_vars)
    return {"data": result}


def extract_rows(graphql_data: dict, root_key: str) -> list[dict]:
    data = graphql_data.get("data", {})
    if root_key not in data:
        raise KeyError(f"Missing data root '{root_key}' in GraphQL response")
    logger.debug("Extracting rows from root_key=%s", root_key)
    return data[root_key] or []
