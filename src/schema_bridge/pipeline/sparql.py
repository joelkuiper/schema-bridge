from __future__ import annotations

from rdflib import Graph

from .resources import load_text
import logging

logger = logging.getLogger("schema_bridge.pipeline.sparql")


def select_rows(graph: Graph, query_path: str) -> list[dict]:
    logger.debug("Running SELECT query: %s", query_path)
    query = load_text(query_path, "schema_bridge.resources")
    rows = []
    for row in graph.query(query):
        rows.append({k: str(v) if v is not None else "" for k, v in row.asdict().items()})
    return rows


def construct_graph(graph: Graph, query_path: str) -> Graph:
    logger.debug("Running CONSTRUCT query: %s", query_path)
    query = load_text(query_path, "schema_bridge.resources")
    return graph.query(query).graph
