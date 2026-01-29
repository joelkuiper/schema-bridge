from __future__ import annotations

import csv
import io
import json
from pathlib import Path
from typing import Iterable

from rdflib import Graph

from .resources import load_text
import logging

logger = logging.getLogger("schema_bridge.pipeline.export")


def construct_dcat(raw_graph: Graph) -> Graph:
    logger.debug("Running DCAT construct query")
    query = load_text("sparql/dcat_construct.sparql", "schema_bridge.resources")
    result = raw_graph.query(query)
    return result.graph


def select_rows(raw_graph: Graph, query_path: str) -> list[dict]:
    logger.debug("Running SELECT query: %s", query_path)
    resolved = query_path if "/" in query_path else f"sparql/{query_path}"
    query = load_text(resolved, "schema_bridge.resources")
    rows = []
    for row in raw_graph.query(query):
        rows.append({k: str(v) if v is not None else "" for k, v in row.asdict().items()})
    return rows


def _stable_rows(rows: Iterable[dict]) -> tuple[list[str], list[dict]]:
    rows_list = list(rows)
    if not rows_list:
        return [], []
    fieldnames = sorted({key for row in rows_list for key in row.keys()})
    rows_sorted = sorted(
        rows_list,
        key=lambda row: tuple(str(row.get(key, "")) for key in fieldnames),
    )
    return fieldnames, rows_sorted


def render_csv(rows: Iterable[dict]) -> str:
    fieldnames, rows_list = _stable_rows(rows)
    if not rows_list:
        return ""
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows_list)
    return output.getvalue()


def write_csv(rows: Iterable[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = render_csv(rows)
    path.write_text(content, encoding="utf-8")


def render_json(data: object) -> str:
    return json.dumps(data, indent=2, sort_keys=True)


def write_json(data: object, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_json(data), encoding="utf-8")


def _normalize_export_format(value: str) -> str:
    normalized = value.strip().lower()
    aliases = {
        "turtle": "ttl",
        "json-ld": "jsonld",
        "rdfxml": "rdfxml",
        "rdf/xml": "rdfxml",
        "xml": "rdfxml",
        "rdf": "rdfxml",
        "ntriples": "nt",
        "n-triples": "nt",
    }
    return aliases.get(normalized, normalized)


def export_formats(
    raw_graph: Graph,
    out_dir: Path | None,
    select_query: str | None,
    construct_query: str | None,
    targets: list[str],
    emit: callable | None = None,
) -> Graph | None:
    targets_set = {_normalize_export_format(target) for target in targets if target.strip()}
    logger.debug("Export targets: %s", sorted(targets_set))
    if out_dir is None and len(targets_set) != 1:
        raise ValueError("Stdout output requires exactly one target format")
    if out_dir is not None:
        logger.debug("Writing outputs to %s", out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
    if "json" in targets_set or "csv" in targets_set:
        if not select_query:
            raise ValueError("Select query is required for CSV/JSON outputs")
        selected = select_rows(raw_graph, select_query)
        logger.debug("Selected %s row(s)", len(selected))
        if "json" in targets_set:
            payload = {"rows": selected}
            if out_dir is None:
                if emit is None:
                    raise ValueError("Stdout output requested but no emitter provided")
                emit(render_json(payload))
            else:
                write_json(payload, out_dir / "resources.json")
        if "csv" in targets_set:
            if out_dir is None:
                if emit is None:
                    raise ValueError("Stdout output requested but no emitter provided")
                emit(render_csv(selected))
            else:
                write_csv(selected, out_dir / "resources.csv")
    construct = None
    rdf_targets = {"ttl", "jsonld", "rdfxml", "nt"} & targets_set
    if rdf_targets:
        if not construct_query:
            raise ValueError("Construct query is required for RDF outputs")
        logger.debug("Running CONSTRUCT query: %s", construct_query)
        resolved = construct_query if "/" in construct_query else f"sparql/{construct_query}"
        construct = raw_graph.query(load_text(resolved, "schema_bridge.resources")).graph
        if "ttl" in targets_set:
            if out_dir is None:
                if emit is None:
                    raise ValueError("Stdout output requested but no emitter provided")
                emit(construct.serialize(format="turtle"))
            else:
                construct.serialize(out_dir / "resources.ttl", format="turtle")
        if "rdfxml" in targets_set:
            if out_dir is None:
                if emit is None:
                    raise ValueError("Stdout output requested but no emitter provided")
                emit(construct.serialize(format="xml"))
            else:
                construct.serialize(out_dir / "resources.rdf", format="xml")
        if "nt" in targets_set:
            if out_dir is None:
                if emit is None:
                    raise ValueError("Stdout output requested but no emitter provided")
                emit(construct.serialize(format="nt"))
            else:
                construct.serialize(out_dir / "resources.nt", format="nt")
    if "jsonld" in targets_set:
        if out_dir is None:
            if emit is None:
                raise ValueError("Stdout output requested but no emitter provided")
            emit(
                construct.serialize(
                    format="json-ld",
                    context=_namespace_context(construct),
                    auto_compact=True,
                )
            )
        else:
            construct.serialize(
                out_dir / "resources.jsonld",
                format="json-ld",
                context=_namespace_context(construct),
                auto_compact=True,
            )
    return construct


def _namespace_context(graph: Graph) -> dict[str, str]:
    context: dict[str, str] = {}
    for prefix, namespace in graph.namespace_manager.namespaces():
        if prefix:
            context[prefix] = str(namespace)
    return context
