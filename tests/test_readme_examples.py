from __future__ import annotations

import os
import shlex
import subprocess
from pathlib import Path

import pytest

README = Path(__file__).parents[1] / "README.md"


def _fixture_for_command(cmd: str) -> Path:
    resources = Path(__file__).parent / "resources"
    if "--profile health-dcat-ap-molgenis" in cmd:
        return resources / "graphql_health_dcat_ap_molgenis.json"
    return resources / "graphql_resources.json"


def _prepare_command(cmd: str, workdir: Path) -> list[str]:
    cmd = cmd.replace("uv run schema-bridge", "python -m schema_bridge.cli")
    cmd = cmd.replace("out/", f"{workdir}/out/")
    cmd = cmd.replace("path/to/mapping.yml", str(Path(__file__).parent / "resources" / "mapping.yml"))
    cmd = cmd.replace("path/to/mapping.rml.ttl", str(Path(__file__).parent / "resources" / "rml_pkg" / "airport_mapping.rml.ttl"))
    cmd = cmd.replace("path/to/data.csv", str(Path(__file__).parent / "resources" / "rml_pkg" / "airport.csv"))
    return shlex.split(cmd)


def _subcommand(cmd: str) -> str | None:
    parts = shlex.split(cmd)
    if "schema-bridge" not in parts:
        return None
    idx = parts.index("schema-bridge")
    return parts[idx + 1] if idx + 1 < len(parts) else None


@pytest.mark.integration
def test_readme_commands(tmp_path: Path) -> None:
    text = README.read_text(encoding="utf-8")
    commands = []
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("- `uv run schema-bridge "):
            commands.append(line[len("- `") : -1])

    assert commands, "No schema-bridge commands found in README"

    for cmd in commands:
        env = os.environ.copy()
        env["SCHEMA_BRIDGE_GRAPHQL_FIXTURE"] = str(_fixture_for_command(cmd))
        src_path = Path(__file__).parents[1] / "src"
        env["PYTHONPATH"] = f"{src_path}{os.pathsep}{env.get('PYTHONPATH', '')}"
        workdir = tmp_path / "case"
        workdir.mkdir(parents=True, exist_ok=True)
        subcommand = _subcommand(cmd)
        if "out/graphql.json" in cmd and subcommand not in {"fetch", "run"}:
            out_dir = workdir / "out"
            out_dir.mkdir(parents=True, exist_ok=True)
            fixture = _fixture_for_command(cmd)
            (out_dir / "graphql.json").write_text(
                fixture.read_text(encoding="utf-8"),
                encoding="utf-8",
            )
        completed = subprocess.run(
            _prepare_command(cmd, workdir),
            cwd=workdir,
            env=env,
            capture_output=True,
            text=True,
        )
        assert completed.returncode == 0, f"Command failed: {cmd}\nSTDOUT: {completed.stdout}\nSTDERR: {completed.stderr}"
