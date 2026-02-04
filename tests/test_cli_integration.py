from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from rdflib import Namespace
from rdflib.namespace import RDF

from schema_bridge.rdf import new_graph


def _base_env() -> dict[str, str]:
    env = os.environ.copy()
    src_path = Path(__file__).parents[1] / "src"
    env["PYTHONPATH"] = f"{src_path}{os.pathsep}{env.get('PYTHONPATH', '')}"
    return env


@pytest.mark.integration
def test_cli_fetch_uses_fixture(tmp_path: Path) -> None:
    resources = Path(__file__).parent / "resources"
    fixture = resources / "graphql_resources.json"
    env = _base_env()
    env["SCHEMA_BRIDGE_GRAPHQL_FIXTURE"] = str(fixture)

    out_path = tmp_path / "out" / "graphql.json"
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "schema_bridge.cli",
            "fetch",
            "--profile",
            "dcat",
            "--limit",
            "2",
            "--out",
            str(out_path),
        ],
        env=env,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, (
        f"fetch failed:\nSTDOUT: {completed.stdout}\nSTDERR: {completed.stderr}"
    )
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert "data" in payload
    fixture_payload = json.loads(fixture.read_text(encoding="utf-8"))
    assert payload["data"]["Resources"] == fixture_payload["data"]["Resources"]


@pytest.mark.integration
def test_cli_export_uses_fixture() -> None:
    resources = Path(__file__).parent / "resources"
    fixture = resources / "graphql_resources.json"
    env = _base_env()
    env["SCHEMA_BRIDGE_GRAPHQL_FIXTURE"] = str(fixture)

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "schema_bridge.cli",
            "export",
            "--profile",
            "dcat",
            "--format",
            "ttl",
            "--limit",
            "2",
        ],
        env=env,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, (
        f"export failed:\nSTDOUT: {completed.stdout}\nSTDERR: {completed.stderr}"
    )
    graph = new_graph()
    graph.parse(data=completed.stdout, format="turtle")
    res = Namespace("https://catalogue.org/")["resource/R1"]
    dct = Namespace("http://purl.org/dc/terms/")
    assert (res, dct["title"], None) in graph


@pytest.mark.integration
def test_cli_export_healthdcat_ap_r5_uses_fixture() -> None:
    resources = Path(__file__).parent / "resources"
    fixture = resources / "graphql_health_dcat_ap_molgenis.json"
    env = _base_env()
    env["SCHEMA_BRIDGE_GRAPHQL_FIXTURE"] = str(fixture)

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "schema_bridge.cli",
            "export",
            "--profile",
            "healthdcat-ap-r5-molgenis",
            "--format",
            "ttl",
            "--limit",
            "2",
        ],
        env=env,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, (
        f"healthdcat export failed:\\nSTDOUT: {completed.stdout}\\nSTDERR: {completed.stderr}"
    )
    graph = new_graph()
    graph.parse(data=completed.stdout, format="turtle")
    res = Namespace("https://catalogue.org/")["resource/MOL-1"]
    dcat = Namespace("http://www.w3.org/ns/dcat#")
    rdf = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
    assert (res, rdf["type"], dcat["Dataset"]) in graph


@pytest.mark.integration
def test_cli_export_canonical_only_uses_fixture() -> None:
    resources = Path(__file__).parent / "resources"
    fixture = resources / "graphql_resources.json"
    env = _base_env()
    env["SCHEMA_BRIDGE_GRAPHQL_FIXTURE"] = str(fixture)

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "schema_bridge.cli",
            "export",
            "--profile",
            "dcat",
            "--format",
            "ttl",
            "--canonical-only",
            "--limit",
            "2",
        ],
        env=env,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, (
        f"canonical export failed:\nSTDOUT: {completed.stdout}\nSTDERR: {completed.stderr}"
    )
    graph = new_graph()
    graph.parse(data=completed.stdout, format="turtle")
    res = Namespace("https://catalogue.org/")["resource/R1"]
    field = Namespace("https://catalogue.org/field/")
    entity = Namespace("https://catalogue.org/entity/")
    dct = Namespace("http://purl.org/dc/terms/")
    assert (res, RDF.type, entity["Resource"]) in graph
    assert (res, field["name"], None) in graph
    assert (res, dct["title"], None) not in graph


@pytest.mark.integration
def test_cli_ingest_dry_run(tmp_path: Path) -> None:
    resources = Path(__file__).parent / "resources" / "profiles" / "ingest-demo"
    input_path = resources / "input.ttl"
    env = _base_env()

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "schema_bridge.cli",
            "ingest",
            str(input_path),
            "--profile",
            str(resources),
            "--dry-run",
        ],
        env=env,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, (
        f"ingest failed:\nSTDOUT: {completed.stdout}\nSTDERR: {completed.stderr}"
    )
    payload = json.loads(completed.stdout)
    assert payload["rows"], "expected rows from ingest dry-run"
    assert payload["rows"][0]["name"] == "Demo Dataset"
    assert payload["rows"][0]["contactEmail"] == "demo@example.org"
