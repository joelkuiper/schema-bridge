from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


def _base_env() -> dict:
    env = os.environ.copy()
    src_path = Path(__file__).parents[1] / "src"
    env["PYTHONPATH"] = f"{src_path}{os.pathsep}{env.get('PYTHONPATH', '')}"
    return env


def test_minimal_profile_smoke(tmp_path: Path) -> None:
    resources = Path(__file__).parent / "resources"
    profile_dir = resources / "minimal"
    graphql_fixture = resources / "graphql_minimal.json"

    env = _base_env()
    env["SCHEMA_BRIDGE_GRAPHQL_FIXTURE"] = str(graphql_fixture)

    convert = subprocess.run(
        [
            sys.executable,
            "-m",
            "schema_bridge.cli",
            "convert",
            str(graphql_fixture),
            "--profile",
            str(profile_dir),
            "--format",
            "json",
        ],
        env=env,
        capture_output=True,
        text=True,
    )
    assert convert.returncode == 0, (
        f"convert failed:\nSTDOUT: {convert.stdout}\nSTDERR: {convert.stderr}"
    )
    assert "\"rows\"" in convert.stdout

    run = subprocess.run(
        [
            sys.executable,
            "-m",
            "schema_bridge.cli",
            "run",
            "--profile",
            str(profile_dir),
            "--format",
            "json",
            "--base-url",
            "https://example.org/",
            "--schema",
            "demo",
            "--limit",
            "1",
        ],
        env=env,
        capture_output=True,
        text=True,
    )
    assert run.returncode == 0, f"run failed:\nSTDERR: {run.stderr}"
    assert "\"rows\"" in run.stdout

    ingest_profile = resources / "ingest"
    ingest_input = ingest_profile / "input.ttl"
    ingest = subprocess.run(
        [
            sys.executable,
            "-m",
            "schema_bridge.ingest_cli",
            "ingest",
            str(ingest_input),
            "--profile",
            str(ingest_profile),
            "--dry-run",
        ],
        env=_base_env(),
        capture_output=True,
        text=True,
    )
    assert ingest.returncode == 0, (
        f"ingest failed:\nSTDOUT: {ingest.stdout}\nSTDERR: {ingest.stderr}"
    )
    payload = json.loads(ingest.stdout)
    assert payload["rows"], "expected rows from ingest"
