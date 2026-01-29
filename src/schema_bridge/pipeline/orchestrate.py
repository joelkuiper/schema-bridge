from __future__ import annotations

from pathlib import Path

from rdflib import Graph

from .export import export_formats
from .resources import load_text
from .shacl import validate_graph
from .profiles import ResolvedExport


def export_and_validate(
    raw_graph: Graph,
    export: ResolvedExport,
    out_dir: Path | None,
    shacl_report: Path | None,
    emit: callable | None = None,
) -> None:
    construct_graph = export_formats(
        raw_graph,
        out_dir,
        export.select_query,
        export.construct_query,
        export.targets,
        emit=emit,
    )
    if export.validate and export.profile.shacl:
        if construct_graph is None:
            if not export.construct_query:
                raise RuntimeError("SHACL validation requires a construct query")
            resolved = (
                export.construct_query
                if "/" in export.construct_query
                else f"sparql/{export.construct_query}"
            )
            construct_graph = raw_graph.query(load_text(resolved, "schema_bridge.resources")).graph
        conforms, report = validate_graph(construct_graph, export.profile.shacl)
        if shacl_report:
            report.serialize(shacl_report, format="turtle")
        if not conforms:
            report_text = report.serialize(format="turtle")
            raise SystemExit(f"SHACL validation failed:\n{report_text}")
