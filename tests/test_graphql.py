from schema_bridge.graphql.client import (
    PaginationConfig,
    _build_updated_filter,
    _paginate_graphql,
    extract_rows,
)
import pytest


def test_paginate_graphql_merges_rows():
    calls = []

    def execute(query, variables):
        calls.append(variables.copy())
        limit = variables["limit"]
        offset = variables["offset"]
        rows = [{"id": f"R{idx}"} for idx in range(offset, min(offset + limit, 5))]
        return {"Resources": rows}

    pagination = PaginationConfig(page_size=2)
    result = _paginate_graphql(
        execute=execute,
        query="query",
        variables=None,
        root_key="Resources",
        pagination=pagination,
        updated_filter=None,
    )
    rows = result["data"]["Resources"]
    assert len(rows) == 5
    assert calls[0]["limit"] == 2
    assert calls[1]["offset"] == 2


def test_paginate_graphql_respects_max_rows():
    def execute(query, variables):
        limit = variables["limit"]
        offset = variables["offset"]
        rows = [{"id": f"R{idx}"} for idx in range(offset, min(offset + limit, 10))]
        return {"Resources": rows}

    pagination = PaginationConfig(page_size=3, max_rows=4)
    result = _paginate_graphql(
        execute=execute,
        query="query",
        variables=None,
        root_key="Resources",
        pagination=pagination,
        updated_filter=None,
    )
    rows = result["data"]["Resources"]
    assert len(rows) == 4


def test_build_updated_filter_between():
    updated = _build_updated_filter("2024-01-01T00:00:00Z", None)
    assert updated is not None
    between = updated["between"]["mg_updatedOn"]
    assert between[0] == "2024-01-01T00:00:00Z"
    assert len(between) == 2


def test_extract_rows_requires_root_key() -> None:
    payload = {"data": {"Other": []}}
    with pytest.raises(KeyError):
        extract_rows(payload, "Resources")
