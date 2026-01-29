from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest
from schema_bridge.profiles import load_profile, load_ingest_profile, resolve_profile_path


def _fixture_for_profile(profile: str) -> Path:
    resources = Path(__file__).parent / "resources"
    if profile == "healthdcat-ap-r5-molgenis":
        return resources / "graphql_health_dcat_ap_molgenis.json"
    return resources / "graphql_resources.json"


def _env_for_profile(profile: str) -> dict[str, str]:
    fixture = _fixture_for_profile(profile)
    env = os.environ.copy()
    src_path = Path(__file__).parents[1] / "src"
    env["PYTHONPATH"] = f"{src_path}{os.pathsep}{env.get('PYTHONPATH', '')}"
    env["SCHEMA_BRIDGE_GRAPHQL_FIXTURE"] = str(fixture)
    return env


def _run_convert(profile: str, output_format: str) -> subprocess.CompletedProcess[str]:
    fixture = _fixture_for_profile(profile)
    env = _env_for_profile(profile)
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "schema_bridge.cli",
            "convert",
            str(fixture),
            "--profile",
            profile,
            "--format",
            output_format,
        ],
        env=env,
        capture_output=True,
        text=True,
    )


def _run_export(
    profile: str, output_format: str, limit: int
) -> subprocess.CompletedProcess[str]:
    env = _env_for_profile(profile)
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "schema_bridge.cli",
            "export",
            "--profile",
            profile,
            "--format",
            output_format,
            "--limit",
            str(limit),
        ],
        env=env,
        capture_output=True,
        text=True,
    )


@pytest.mark.parametrize(
    ("profile", "output_format", "marker"),
    [
        ("dcat", "json", '"rows"'),
        ("schemaorg-molgenis", "ttl", "@prefix"),
        ("healthdcat-ap-r5-molgenis", "ttl", "@prefix"),
    ],
)
def test_profile_outputs_to_stdout(
    tmp_path: Path, profile: str, output_format: str, marker: str
) -> None:
    completed = _run_convert(profile, output_format)
    assert completed.returncode == 0, (
        f"{profile} failed:\nSTDOUT: {completed.stdout}\nSTDERR: {completed.stderr}"
    )
    assert marker in completed.stdout


@pytest.mark.integration
def test_molgenis_profile_export_uses_fixture() -> None:
    completed = _run_export("healthdcat-ap-r5-molgenis", "ttl", limit=50)
    assert completed.returncode == 0, (
        f"run failed:\nSTDOUT: {completed.stdout}\nSTDERR: {completed.stderr}"
    )
    assert "@prefix" in completed.stdout


def test_profile_directory_resolution() -> None:
    resources = Path(__file__).parent / "resources" / "minimal"
    profile = load_profile(str(resources), expected_kind="export")
    assert profile.graphql_query is not None
    resolved = resolve_profile_path(profile, profile.graphql_query, "schema_bridge.resources")
    assert Path(resolved).exists()


def test_profile_file_resolution() -> None:
    profile_path = Path(__file__).parent / "resources" / "minimal" / "profile.yml"
    profile = load_profile(str(profile_path), expected_kind="export")
    assert profile.name == "minimal-demo"


def test_ingest_profile_loads_required_sections() -> None:
    ingest_profile = Path(__file__).parent / "resources" / "profiles" / "ingest-demo"
    profile = load_ingest_profile(str(ingest_profile))
    assert profile.select_query is not None
    assert profile.table is not None
