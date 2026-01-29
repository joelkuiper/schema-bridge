from __future__ import annotations

from pathlib import Path

from schema_bridge.resources import load_text, load_yaml, resolve_resource_path


def test_load_text_from_packaged_profile() -> None:
    query = load_text("profiles/dcat/sparql/select.sparql", "schema_bridge.resources")
    assert "SELECT" in query
    assert "field:" in query


def test_load_yaml_from_profile_file() -> None:
    profile = load_yaml("profiles/dcat/profile.yml", "schema_bridge.resources")
    assert profile.get("kind") == "export"


def test_resolve_resource_path_for_local_file(tmp_path: Path) -> None:
    path = tmp_path / "local.txt"
    path.write_text("hello", encoding="utf-8")
    resolved = resolve_resource_path(str(path), "schema_bridge.resources")
    assert resolved == str(path.resolve())
