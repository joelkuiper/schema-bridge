from __future__ import annotations

from importlib import resources
from pathlib import Path

import yaml


def resolve_resource_path(path: str, package: str) -> str:
    candidate = Path(path)
    if candidate.exists():
        return str(candidate.resolve())
    return str(resources.files(package).joinpath(path))


def load_text(path: str, package: str) -> str:
    return Path(resolve_resource_path(path, package)).read_text(encoding="utf-8")


def load_yaml(path: str, package: str) -> dict:
    content = load_text(path, package)
    data = yaml.safe_load(content)
    if not isinstance(data, dict):
        raise ValueError(f"Expected mapping in YAML file: {path}")
    return data
