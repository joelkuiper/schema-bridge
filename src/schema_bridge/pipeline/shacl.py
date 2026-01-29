from __future__ import annotations

from dataclasses import dataclass

from rdflib import Graph

from schema_bridge.rdf import new_graph

from .resources import load_text
import logging

logger = logging.getLogger("schema_bridge.pipeline.shacl")


@dataclass
class ShaclConfig:
    shapes: str
    validate: bool = True


def load_graph_from_shacl(path: str) -> Graph:
    logger.debug("Loading SHACL shapes: %s", path)
    graph = new_graph()
    data = load_text(path, "schema_bridge.resources")
    graph.parse(data=data, format="turtle")
    return graph


def validate_graph(data_graph: Graph, shacl_config: ShaclConfig) -> tuple[bool, Graph]:
    from pyshacl import validate

    logger.debug("Validating graph with SHACL: %s", shacl_config.shapes)
    shacl_graph = load_graph_from_shacl(shacl_config.shapes)
    conforms, report_graph, _ = validate(
        data_graph=data_graph,
        shacl_graph=shacl_graph,
        inference="rdfs",
        abort_on_first=False,
        meta_shacl=False,
        advanced=True,
        debug=False,
    )
    logger.debug("SHACL conforms=%s", conforms)
    return bool(conforms), report_graph
