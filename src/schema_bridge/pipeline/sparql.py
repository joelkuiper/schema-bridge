from __future__ import annotations

from rdflib import Graph
from typing import Any, Iterable, cast

from .resources import load_text
import logging

logger = logging.getLogger("schema_bridge.pipeline.sparql")


def select_rows(graph: Graph, query_path: str) -> list[dict]:
    logger.debug("Running SELECT query: %s", query_path)
    query = load_text(query_path, "schema_bridge.resources")
    rows = []
    result = cast(Iterable[Any], graph.query(query))
    for row in result:
        row_dict = cast(dict[str, Any], row.asdict())
        rows.append({k: str(v) if v is not None else "" for k, v in row_dict.items()})
    return rows


def construct_graph(graph: Graph, query_path: str) -> Graph:
    logger.debug("Running CONSTRUCT query: %s", query_path)
    query = load_text(query_path, "schema_bridge.resources")
    result = graph.query(query)
    if result.graph is None:
        raise RuntimeError("CONSTRUCT query did not return a graph")
    return result.graph
