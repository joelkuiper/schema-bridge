from __future__ import annotations

import json

from schema_bridge.rdf import MappingConfig, RawMapping, export_formats, load_raw_from_rows, new_graph


def test_export_jsonld_includes_context_and_graph() -> None:
    raw = new_graph()
    rows = [
        {
            "id": "R1",
            "name": "Resource One",
            "description": "Example description",
            "website": "https://example.org",
        }
    ]
    load_raw_from_rows(rows, raw, MappingConfig(raw=RawMapping()))

    captured: list[str] = []
    export_formats(
        raw,
        out_dir=None,
        select_query=None,
        construct_query="profiles/dcat/sparql/construct.sparql",
        targets=["jsonld"],
        emit=captured.append,
    )
    assert captured, "expected JSON-LD output"
    payload = json.loads(captured[0])
    assert isinstance(payload, dict)
    assert "@context" in payload
    assert any(key in payload for key in ("@graph", "@id"))
    context = payload["@context"]
    assert isinstance(context, dict)
    assert "dcat" in context or "dct" in context
