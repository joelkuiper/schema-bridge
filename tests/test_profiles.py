from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest


def _fixture_for_profile(profile: str) -> Path:
    resources = Path(__file__).parent / "resources"
    if profile == "health-dcat-ap-molgenis":
        return resources / "graphql_health_dcat_ap_molgenis.json"
    return resources / "graphql_resources.json"


def _run_convert(
    tmp_path: Path, profile: str, output_format: str
) -> subprocess.CompletedProcess[str]:
    fixture = _fixture_for_profile(profile)
    env = os.environ.copy()
    src_path = Path(__file__).parents[1] / "src"
    env["PYTHONPATH"] = f"{src_path}{os.pathsep}{env.get('PYTHONPATH', '')}"
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


@pytest.mark.parametrize(
    ("profile", "output_format", "marker"),
    [
        ("dcat", "json", "\"rows\""),
        ("fdp", "ttl", "@prefix"),
        ("health-dcat-ap-molgenis", "ttl", "@prefix"),
    ],
)
def test_profile_outputs_to_stdout(
    tmp_path: Path, profile: str, output_format: str, marker: str
) -> None:
    completed = _run_convert(tmp_path, profile, output_format)
    assert completed.returncode == 0, (
        f"{profile} failed:\nSTDOUT: {completed.stdout}\nSTDERR: {completed.stderr}"
    )
    assert marker in completed.stdout
