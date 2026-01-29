from __future__ import annotations

from rdflib import Graph


def new_graph(store: str | None = None) -> Graph:
    store_name = store or "Oxigraph"
    if store_name:
        return Graph(store=store_name)
    return Graph()
