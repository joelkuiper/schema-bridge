from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def _fixture_for_profile(profile: str) -> Path:
    resources = Path(__file__).parent / "resources"
    if profile == "health-dcat-ap":
        return resources / "graphql_health_dcat_ap.json"
    if profile == "health-ri-core-v2":
        return resources / "graphql_health_ri_core_v2.json"
    if profile == "dcat-all-attributes":
        return resources / "graphql_dcat_all_resources.json"
    return resources / "graphql_resources.json"


def _run_convert(tmp_path: Path, profile: str) -> subprocess.CompletedProcess[str]:
    fixture = _fixture_for_profile(profile)
    out_dir = tmp_path / profile
    out_dir.mkdir(parents=True, exist_ok=True)
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
            "-o",
            str(out_dir),
        ],
        env=env,
        capture_output=True,
        text=True,
    )


def test_profile_fdp_outputs(tmp_path: Path) -> None:
    completed = _run_convert(tmp_path, "fdp")
    assert completed.returncode == 0, f"fdp failed:\nSTDOUT: {completed.stdout}\nSTDERR: {completed.stderr}"
    out_dir = tmp_path / "fdp"
    assert (out_dir / "resources.jsonld").exists()
    assert (out_dir / "resources.ttl").exists()


def test_profile_dcat_outputs(tmp_path: Path) -> None:
    completed = _run_convert(tmp_path, "dcat")
    assert completed.returncode == 0, f"dcat failed:\nSTDOUT: {completed.stdout}\nSTDERR: {completed.stderr}"
    out_dir = tmp_path / "dcat"
    assert (out_dir / "resources.csv").exists()
    assert (out_dir / "resources.json").exists()
    assert (out_dir / "resources.jsonld").exists()
    assert (out_dir / "resources.ttl").exists()


def test_profile_dcat_ap_3_0_1_outputs(tmp_path: Path) -> None:
    completed = _run_convert(tmp_path, "dcat-ap-3.0.1")
    assert completed.returncode == 0, (
        "dcat-ap-3.0.1 failed:\nSTDOUT: "
        f"{completed.stdout}\nSTDERR: {completed.stderr}"
    )
    out_dir = tmp_path / "dcat-ap-3.0.1"
    assert (out_dir / "resources.csv").exists()
    assert (out_dir / "resources.json").exists()
    assert (out_dir / "resources.jsonld").exists()
    assert (out_dir / "resources.ttl").exists()


def test_profile_health_dcat_ap_outputs(tmp_path: Path) -> None:
    completed = _run_convert(tmp_path, "health-dcat-ap")
    assert completed.returncode == 0, (
        "health-dcat-ap failed:\nSTDOUT: "
        f"{completed.stdout}\nSTDERR: {completed.stderr}"
    )
    out_dir = tmp_path / "health-dcat-ap"
    assert (out_dir / "resources.csv").exists()
    assert (out_dir / "resources.json").exists()
    assert (out_dir / "resources.jsonld").exists()
    assert (out_dir / "resources.ttl").exists()


def test_profile_health_ri_core_v2_outputs(tmp_path: Path) -> None:
    completed = _run_convert(tmp_path, "health-ri-core-v2")
    assert completed.returncode == 0, (
        "health-ri-core-v2 failed:\nSTDOUT: "
        f"{completed.stdout}\nSTDERR: {completed.stderr}"
    )
    out_dir = tmp_path / "health-ri-core-v2"
    assert (out_dir / "resources.csv").exists()
    assert (out_dir / "resources.json").exists()
    assert (out_dir / "resources.jsonld").exists()
    assert (out_dir / "resources.ttl").exists()


def test_profile_dcat_all_attributes_outputs(tmp_path: Path) -> None:
    completed = _run_convert(tmp_path, "dcat-all-attributes")
    assert completed.returncode == 0, (
        "dcat-all-attributes failed:\nSTDOUT: "
        f"{completed.stdout}\nSTDERR: {completed.stderr}"
    )
    out_dir = tmp_path / "dcat-all-attributes"
    assert (out_dir / "resources.jsonld").exists()
    assert (out_dir / "resources.ttl").exists()
