from __future__ import annotations

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from schema_bridge.profiles.loader import ProfileConfig


def resolve_graphql_target(
    *,
    profile: "ProfileConfig",
    base_url: str | None,
    schema: str | None,
    endpoint: str | None,
) -> tuple[str | None, str | None, str | None]:
    resolved_endpoint = (
        endpoint
        or profile.graphql_endpoint
        or os.getenv("SCHEMA_BRIDGE_GRAPHQL_ENDPOINT")
    )
    resolved_base_url = (
        base_url or profile.base_url or os.getenv("SCHEMA_BRIDGE_BASE_URL")
    )
    resolved_schema = schema or profile.schema or os.getenv("SCHEMA_BRIDGE_SCHEMA")
    if not resolved_endpoint and (not resolved_base_url or not resolved_schema):
        if not os.getenv("SCHEMA_BRIDGE_GRAPHQL_FIXTURE"):
            raise SystemExit(
                "GraphQL endpoint is required. Provide --graphql-endpoint or "
                "--base-url/--schema (or set them in the profile or environment)."
            )
    return resolved_endpoint, resolved_base_url, resolved_schema
