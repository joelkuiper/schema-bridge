from __future__ import annotations

from dataclasses import dataclass

from rdflib import Graph

from .resources import load_text


@dataclass
class ShaclConfig:
    shapes: str
    validate: bool = True


def load_graph_from_shacl(path: str) -> Graph:
    graph = Graph()
    data = load_text(path, "schema_bridge.resources")
    graph.parse(data=data, format="turtle")
    return graph


def validate_graph(data_graph: Graph, shacl_config: ShaclConfig) -> tuple[bool, Graph]:
    from pyshacl import validate

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
    return bool(conforms), report_graph
