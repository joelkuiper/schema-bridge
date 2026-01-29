from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Mapping
from urllib.parse import quote

from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF

EX = Namespace("https://catalogue.org/")
FIELD = Namespace("https://catalogue.org/field/")
ENTITY = Namespace("https://catalogue.org/entity/")


@dataclass
class RawMapping:
    base_uri: str = str(EX)
    field_ns: str = str(FIELD)
    entity_ns: str = str(ENTITY)
    entity_name: str = "Resource"
    id_field: str = "id"
    subject_path: str = "resource"


@dataclass
class MappingConfig:
    raw: RawMapping = field(default_factory=RawMapping)
    field_aliases: dict[str, str] = field(default_factory=dict)
    iri_fields: set[str] = field(default_factory=set)
    field_paths: dict[str, str] = field(default_factory=dict)
    drop_nested: bool = False

    @classmethod
    def from_dict(cls, data: Mapping[str, object] | None) -> "MappingConfig":
        if not data:
            return cls()
        raw = RawMapping(
            base_uri=str(data.get("base_uri", EX)),
            field_ns=str(data.get("field_ns", FIELD)),
            entity_ns=str(data.get("entity_ns", ENTITY)),
            entity_name=str(data.get("entity_name", "Resource")),
            id_field=str(data.get("id_field", "id")),
            subject_path=str(data.get("subject_path", "resource")),
        )
        field_aliases = {
            str(k): str(v)
            for k, v in (data.get("field_aliases") or {}).items()  # type: ignore[union-attr]
        }
        iri_fields = {str(item) for item in (data.get("iri_fields") or [])}  # type: ignore[arg-type]
        field_paths = {
            str(k): str(v)
            for k, v in (data.get("field_paths") or {}).items()  # type: ignore[union-attr]
        }
        drop_nested = bool(data.get("drop_nested", False))
        return cls(
            raw=raw,
            field_aliases=field_aliases,
            iri_fields=iri_fields,
            field_paths=field_paths,
            drop_nested=drop_nested,
        )


def _iter_values(value: object) -> Iterable[object]:
    if isinstance(value, (list, tuple, set)):
        return value
    return (value,)


def _coerce_object(value: object, use_iri: bool) -> URIRef | Literal:
    if use_iri:
        return URIRef(str(value))
    return Literal(value)


def _parse_path(path: str) -> list[tuple[str, bool]]:
    parts: list[tuple[str, bool]] = []
    for raw in path.split("."):
        if raw.endswith("[]"):
            parts.append((raw[:-2], True))
        else:
            parts.append((raw, False))
    return parts


def _values_from_path(value: object, path: str) -> list[object]:
    parts = _parse_path(path)

    def walk(current: object, idx: int) -> list[object]:
        if idx >= len(parts):
            return [current]
        key, is_list = parts[idx]
        if isinstance(current, dict):
            next_value = current.get(key)
        else:
            return []
        if is_list:
            if not isinstance(next_value, list):
                return []
            results: list[object] = []
            for item in next_value:
                results.extend(walk(item, idx + 1))
            return results
        return walk(next_value, idx + 1)

    return [item for item in walk(value, 0) if item is not None]


def _is_nested(value: object) -> bool:
    if isinstance(value, dict):
        return True
    if isinstance(value, list):
        return any(isinstance(item, dict) for item in value)
    return False


def _normalized_row(row: dict, mapping: MappingConfig) -> dict:
    normalized = dict(row)
    for out_key, path in mapping.field_paths.items():
        values = _values_from_path(row, path)
        if not values:
            continue
        if len(values) == 1:
            normalized[out_key] = values[0]
        else:
            normalized[out_key] = values
    if mapping.drop_nested:
        normalized = {k: v for k, v in normalized.items() if not _is_nested(v)}
    return normalized


def _resolve_id_alias(normalized: dict, mapping: MappingConfig) -> dict:
    if mapping.raw.id_field in normalized:
        return normalized
    for key, alias in mapping.field_aliases.items():
        if alias == mapping.raw.id_field and key in normalized:
            normalized[mapping.raw.id_field] = normalized[key]
            return normalized
    return normalized


def load_raw_from_rows(rows: Iterable[dict], graph: Graph, mapping: MappingConfig) -> None:
    entity_type = URIRef(f"{mapping.raw.entity_ns}{mapping.raw.entity_name}")
    for row in rows:
        normalized = _resolve_id_alias(_normalized_row(row, mapping), mapping)
        if mapping.raw.id_field not in normalized:
            raise ValueError(f"Missing id field '{mapping.raw.id_field}' in row")
        subject_id = quote(str(normalized[mapping.raw.id_field]), safe="")
        subject = URIRef(f"{mapping.raw.base_uri}{mapping.raw.subject_path}/{subject_id}")
        graph.add((subject, RDF.type, entity_type))
        for key, value in normalized.items():
            mapped_key = mapping.field_aliases.get(key, key)
            predicate = URIRef(f"{mapping.raw.field_ns}{mapped_key}")
            use_iri = mapped_key in mapping.iri_fields
            for item in _iter_values(value):
                if item is None:
                    continue
                graph.add((subject, predicate, _coerce_object(item, use_iri)))
