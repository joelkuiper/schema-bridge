from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Mapping
from urllib.parse import quote

from rdflib import Graph, Namespace, URIRef, Literal, BNode
from rdflib.namespace import RDF, SKOS, OWL
import logging

logger = logging.getLogger("schema_bridge.rdf.mapping")

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
    field_paths: dict[str, list[str] | str] = field(default_factory=dict)
    concept_fields: dict[str, "ConceptField"] = field(default_factory=dict)
    node_fields: dict[str, "NodeField"] = field(default_factory=dict)
    auto_nodes: bool = True
    node_defaults: "NodeDefaults" = field(default_factory=lambda: NodeDefaults())
    id_strategy: "IdStrategy" = field(default_factory=lambda: IdStrategy())
    concept_ns: str = "https://catalogue.org/concept/"
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
        field_paths: dict[str, list[str] | str] = {}
        for key, value in (data.get("field_paths") or {}).items():  # type: ignore[union-attr]
            if isinstance(value, list):
                field_paths[str(key)] = [str(item) for item in value]
            else:
                field_paths[str(key)] = str(value)
        concept_fields = {}
        for key, raw_cfg in (data.get("concept_fields") or {}).items():  # type: ignore[union-attr]
            if isinstance(raw_cfg, dict):
                concept_fields[str(key)] = ConceptField.from_dict(raw_cfg)
        node_fields = {}
        for key, raw_cfg in (data.get("node_fields") or {}).items():  # type: ignore[union-attr]
            if isinstance(raw_cfg, dict):
                node_fields[str(key)] = NodeField.from_dict(raw_cfg)
        concept_ns = str(data.get("concept_ns", "https://catalogue.org/concept/"))
        drop_nested = bool(data.get("drop_nested", False))
        auto_nodes = bool(data.get("auto_nodes", True))
        raw_node_defaults = data.get("node_defaults")
        node_defaults = NodeDefaults.from_dict(
            raw_node_defaults if isinstance(raw_node_defaults, Mapping) else None
        )
        raw_id_strategy = data.get("id_strategy")
        id_strategy = IdStrategy.from_dict(
            raw_id_strategy if isinstance(raw_id_strategy, Mapping) else None
        )
        return cls(
            raw=raw,
            field_aliases=field_aliases,
            iri_fields=iri_fields,
            field_paths=field_paths,
            concept_fields=concept_fields,
            node_fields=node_fields,
            auto_nodes=auto_nodes,
            node_defaults=node_defaults,
            id_strategy=id_strategy,
            concept_ns=concept_ns,
            drop_nested=drop_nested,
        )


@dataclass(frozen=True)
class ConceptField:
    path: str
    predicate: str | None = None
    uri_path: str = "ontologyTermURI"
    code_path: str = "code"
    label_path: str = "name"
    lang: str | None = None

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> "ConceptField":
        predicate = _opt_str(data.get("predicate"))
        return cls(
            path=str(data.get("path", "")),
            predicate=predicate if predicate else None,
            uri_path=str(data.get("uri", data.get("uri_path", "ontologyTermURI"))),
            code_path=str(data.get("code", data.get("code_path", "code"))),
            label_path=str(data.get("label", data.get("label_path", "name"))),
            lang=_opt_str(data.get("lang")),
        )


@dataclass(frozen=True)
class NodeField:
    path: str
    predicate: str
    subject_path: str
    id_field: str = "id"
    type_iri: str | None = None
    fields: dict[str, str] = field(default_factory=dict)
    iri_fields: set[str] = field(default_factory=set)

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> "NodeField":
        fields_raw = data.get("fields")
        fields = fields_raw if isinstance(fields_raw, dict) else {}
        iri_fields_raw = data.get("iri_fields") or []
        return cls(
            path=str(data.get("path", "")),
            predicate=str(data.get("predicate", "")),
            subject_path=str(data.get("subject_path", "")),
            id_field=str(data.get("id_field", "id")),
            type_iri=str(data.get("type_iri")) if data.get("type_iri") else None,
            fields={str(k): str(v) for k, v in fields.items()},
            iri_fields={str(item) for item in iri_fields_raw}
            if isinstance(iri_fields_raw, list)
            else set(),
        )


@dataclass(frozen=True)
class NormalizeConfig:
    trim: bool = True
    lowercase: bool = False
    url_encode: bool = True

    @classmethod
    def from_dict(cls, data: Mapping[str, object] | None) -> "NormalizeConfig":
        if not data:
            return cls()
        return cls(
            trim=bool(data.get("trim", True)),
            lowercase=bool(data.get("lowercase", False)),
            url_encode=bool(data.get("url_encode", True)),
        )


@dataclass(frozen=True)
class IdStrategy:
    mode: str = "template"
    template: str | None = None
    pid_fields: list[str] = field(default_factory=list)
    fallback_fields: list[str] = field(default_factory=list)
    normalize: NormalizeConfig = field(default_factory=NormalizeConfig)

    @classmethod
    def from_dict(cls, data: Mapping[str, object] | None) -> "IdStrategy":
        if not data:
            return cls()
        pid_fields = data.get("pid_fields")
        fallback_fields = data.get("fallback_fields")
        raw_normalize = data.get("normalize")
        return cls(
            mode=str(data.get("mode", "template")),
            template=str(data.get("template")) if data.get("template") else None,
            pid_fields=[str(item) for item in pid_fields] if isinstance(pid_fields, list) else [],
            fallback_fields=[str(item) for item in fallback_fields] if isinstance(fallback_fields, list) else [],
            normalize=NormalizeConfig.from_dict(
                raw_normalize if isinstance(raw_normalize, Mapping) else None
            ),
        )


@dataclass(frozen=True)
class NodeDefaults:
    subject_template: str | None = None
    id_fields: list[str] = field(default_factory=list)
    normalize: NormalizeConfig = field(default_factory=NormalizeConfig)

    @classmethod
    def from_dict(cls, data: Mapping[str, object] | None) -> "NodeDefaults":
        if not data:
            return cls()
        id_fields = data.get("id_fields")
        raw_normalize = data.get("normalize")
        return cls(
            subject_template=str(data.get("subject_template")) if data.get("subject_template") else None,
            id_fields=[str(item) for item in id_fields] if isinstance(id_fields, list) else [],
            normalize=NormalizeConfig.from_dict(
                raw_normalize if isinstance(raw_normalize, Mapping) else None
            ),
        )


def _iter_values(value: object) -> Iterable[object]:
    if isinstance(value, (list, tuple, set)):
        return value
    return (value,)


def _coerce_object(value: object, use_iri: bool) -> URIRef | Literal:
    if use_iri:
        return URIRef(str(value))
    return Literal(value)


def _opt_str(value: object | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


def _normalize_value(value: object | None, normalize: NormalizeConfig) -> str | None:
    if value is None:
        return None
    text = str(value)
    if normalize.trim:
        text = text.strip()
    if normalize.lowercase:
        text = text.lower()
    return text


def _select_id_value(row: Mapping[str, object], fields: list[str]) -> str | None:
    if not fields:
        return None
    for field_name in fields:
        value = row.get(field_name)
        if value is None:
            continue
        if isinstance(value, (list, dict)):
            continue
        text = str(value)
        if text:
            return text
    return None


def _format_subject(
    *,
    template: str | None,
    base_uri: str,
    path: str,
    value: str,
    normalize: NormalizeConfig,
) -> URIRef:
    normalized = _normalize_value(value, normalize) or value
    if normalize.url_encode:
        normalized = quote(normalized, safe="")
    if template:
        subject = template.format(base_uri=base_uri, path=path, id=normalized)
        return URIRef(subject)
    return URIRef(f"{base_uri}{path}/{normalized}")


def _subject_from_row(row: Mapping[str, object], mapping: MappingConfig) -> URIRef:
    strategy = mapping.id_strategy
    if not strategy.pid_fields and not strategy.fallback_fields:
        raise ValueError("Missing id_strategy configuration (pid_fields or fallback_fields)")
    pid = _select_id_value(row, strategy.pid_fields)
    fallback = _select_id_value(row, strategy.fallback_fields)
    chosen = pid or fallback
    if not chosen:
        raise ValueError("Missing stable identifier; configure id_strategy.pid_fields/fallback_fields")
    return _format_subject(
        template=strategy.template,
        base_uri=mapping.raw.base_uri,
        path=mapping.raw.subject_path,
        value=chosen,
        normalize=strategy.normalize,
    )


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


def _value_from_path(value: object, path: str) -> object | None:
    values = _values_from_path(value, path)
    if not values:
        return None
    return values[0]


def _is_nested(value: object) -> bool:
    if isinstance(value, dict):
        return True
    if isinstance(value, list):
        return any(isinstance(item, dict) for item in value)
    return False


def _normalized_row(row: dict, mapping: MappingConfig) -> dict:
    normalized = dict(row)
    for out_key, path_spec in mapping.field_paths.items():
        paths = path_spec if isinstance(path_spec, list) else [path_spec]
        merged: list[object] = []
        for path in paths:
            values = _values_from_path(row, path)
            if values:
                merged.extend(values)
        merged = [value for value in merged if not isinstance(value, (dict, list))]
        if not merged:
            continue
        existing = normalized.get(out_key)
        if existing is None:
            if len(merged) == 1:
                normalized[out_key] = merged[0]
            else:
                normalized[out_key] = merged
            continue
        if isinstance(existing, list):
            existing.extend(merged)
            normalized[out_key] = existing
        else:
            normalized[out_key] = [existing, *merged]
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


def _concept_iri(
    *,
    value: object,
    mapping: MappingConfig,
    uri_path: str,
    code_path: str,
    label_path: str,
) -> tuple[URIRef | BNode, str | None, str | None, str | None]:
    uri_value = None
    code_value = None
    label_value = None
    if isinstance(value, dict):
        uri_value = _value_from_path(value, uri_path)
        code_value = _value_from_path(value, code_path)
        label_value = _value_from_path(value, label_path)
    elif isinstance(value, str):
        if value.startswith("http://") or value.startswith("https://"):
            uri_value = value
        else:
            label_value = value
    if uri_value:
        return (
            URIRef(str(uri_value)),
            str(code_value) if code_value else None,
            str(label_value) if label_value else None,
            str(uri_value),
        )
    if code_value:
        code_str = str(code_value)
        return (
            URIRef(f"{mapping.concept_ns}{quote(code_str, safe='')}"),
            code_str,
            str(label_value) if label_value else None,
            None,
        )
    if label_value:
        label_str = str(label_value)
        return (
            URIRef(f"{mapping.concept_ns}{quote(label_str, safe='')}"),
            None,
            label_str,
            None,
        )
    return BNode(), None, None, None


def _add_concepts(
    subject: URIRef, row: dict, graph: Graph, mapping: MappingConfig
) -> None:
    for key, cfg in mapping.concept_fields.items():
        if not cfg.path:
            continue
        predicate_name = cfg.predicate or mapping.field_aliases.get(key, key)
        predicate = URIRef(f"{mapping.raw.field_ns}{predicate_name}")
        for item in _values_from_path(row, cfg.path):
            concept, code, label, uri_value = _concept_iri(
                value=item,
                mapping=mapping,
                uri_path=cfg.uri_path,
                code_path=cfg.code_path,
                label_path=cfg.label_path,
            )
            graph.add((subject, predicate, concept))
            graph.add((concept, RDF.type, SKOS.Concept))
            if label:
                graph.add((concept, SKOS.prefLabel, Literal(label, lang=cfg.lang)))
            if code:
                graph.add((concept, SKOS.notation, Literal(code)))
            if uri_value and str(concept) != uri_value:
                graph.add((concept, OWL.sameAs, URIRef(uri_value)))


def _add_nodes(
    subject: URIRef, row: dict, graph: Graph, mapping: MappingConfig
) -> None:
    for cfg in mapping.node_fields.values():
        if not cfg.path or not cfg.predicate or not cfg.subject_path:
            continue
        predicate = URIRef(f"{mapping.raw.field_ns}{cfg.predicate}")
        for item in _values_from_path(row, cfg.path):
            if not isinstance(item, dict):
                continue
            node_id = _value_from_path(item, cfg.id_field)
            if node_id is None:
                continue
            node = URIRef(
                f"{mapping.raw.base_uri}{cfg.subject_path}/{quote(str(node_id), safe='')}"
            )
            graph.add((subject, predicate, node))
            if cfg.type_iri:
                graph.add((node, RDF.type, URIRef(cfg.type_iri)))
            for key, value in item.items():
                if cfg.fields:
                    if key not in cfg.fields:
                        continue
                    mapped_key = cfg.fields[key]
                else:
                    mapped_key = key
                pred = URIRef(f"{mapping.raw.field_ns}{mapped_key}")
                use_iri = mapped_key in cfg.iri_fields
                for item_value in _iter_values(value):
                    if item_value is None:
                        continue
                    graph.add((node, pred, _coerce_object(item_value, use_iri)))


def _auto_node_subject(
    *,
    path: str,
    item: Mapping[str, object],
    mapping: MappingConfig,
) -> URIRef | BNode:
    node_id = _select_id_value(item, mapping.node_defaults.id_fields)
    if not node_id:
        return BNode()
    return _format_subject(
        template=mapping.node_defaults.subject_template,
        base_uri=mapping.raw.base_uri,
        path=path,
        value=node_id,
        normalize=mapping.node_defaults.normalize,
    )


def _add_auto_nodes(subject: URIRef, row: dict, graph: Graph, mapping: MappingConfig) -> None:
    if not mapping.auto_nodes:
        return
    for key, value in row.items():
        if isinstance(value, dict):
            items = [value]
        elif isinstance(value, list) and any(isinstance(item, dict) for item in value):
            items = [item for item in value if isinstance(item, dict)]
        else:
            continue
        predicate_name = mapping.field_aliases.get(key, key)
        predicate = URIRef(f"{mapping.raw.field_ns}{predicate_name}")
        for item in items:
            node = _auto_node_subject(path=key, item=item, mapping=mapping)
            graph.add((subject, predicate, node))
            for item_key, item_value in item.items():
                pred = URIRef(f"{mapping.raw.field_ns}{item_key}")
                for item_val in _iter_values(item_value):
                    if item_val is None:
                        continue
                    graph.add((node, pred, _coerce_object(item_val, False)))


def load_raw_from_rows(
    rows: Iterable[dict], graph: Graph, mapping: MappingConfig
) -> None:
    rows_list = list(rows)
    logger.debug(
        "Loading %s row(s) into RDF graph for %s",
        len(rows_list),
        mapping.raw.entity_name,
    )
    entity_type = URIRef(f"{mapping.raw.entity_ns}{mapping.raw.entity_name}")
    for row in rows_list:
        normalized = _resolve_id_alias(_normalized_row(row, mapping), mapping)
        subject = _subject_from_row(normalized, mapping)
        graph.add((subject, RDF.type, entity_type))
        for key, value in normalized.items():
            mapped_key = mapping.field_aliases.get(key, key)
            predicate = URIRef(f"{mapping.raw.field_ns}{mapped_key}")
            use_iri = mapped_key in mapping.iri_fields
            for item in _iter_values(value):
                if item is None:
                    continue
                graph.add((subject, predicate, _coerce_object(item, use_iri)))
        if mapping.concept_fields:
            _add_concepts(subject, row, graph, mapping)
        if mapping.node_fields:
            _add_nodes(subject, row, graph, mapping)
        if mapping.auto_nodes:
            _add_auto_nodes(subject, row, graph, mapping)
