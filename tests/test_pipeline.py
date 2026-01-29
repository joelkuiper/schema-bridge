import csv
import io
from pathlib import Path
from rdflib import Namespace, URIRef
from rdflib.namespace import RDF

from schema_bridge.rdf import new_graph

import pytest
from schema_bridge.rdf import (
    MappingConfig,
    RawMapping,
    construct_dcat,
    export_formats,
    load_raw_from_rows,
    render_csv,
    select_rows,
    validate_graph,
)
from schema_bridge.rdf.mapping import ConceptField, NodeField
from schema_bridge.resources import load_text, load_yaml
from schema_bridge.profiles import load_mapping_override, load_profile, resolve_profile_path
from schema_bridge.workflows import materialize_rml

EX = Namespace("https://catalogue.org/")
FIELD = Namespace("https://catalogue.org/field/")
ENTITY = Namespace("https://catalogue.org/entity/")


def test_raw_load_and_dcat_construct():
    raw = new_graph()
    rows = [
        {
            "id": "R1",
            "name": "My Resource",
            "description": "Example resource",
            "contactEmail": "foo@org.org",
            "website": "https://example.org",
        }
    ]
    load_raw_from_rows(rows, raw, MappingConfig(raw=RawMapping()))

    res = EX["resource/R1"]
    assert (res, RDF.type, ENTITY["Resource"]) in raw
    assert (res, FIELD["name"], None) in raw

    dcat = construct_dcat(raw)
    DCAT = Namespace("http://www.w3.org/ns/dcat#")
    DCT = Namespace("http://purl.org/dc/terms/")
    VCARD = Namespace("http://www.w3.org/2006/vcard/ns#")

    assert (res, RDF.type, DCAT["Dataset"]) in dcat
    assert (res, DCT["title"], None) in dcat

    assert (None, RDF.type, VCARD["Individual"]) in dcat


def test_catalogs_use_case_dcat_fields():
    raw = new_graph()
    rows = [
        {
            "id": "C1",
            "title": "Rare Disease Catalogue",
            "description": "Registry of rare disease datasets",
            "theme": [
                "http://eurovoc.europa.eu/100141",
                "http://eurovoc.europa.eu/100142",
            ],
            "keyword": ["rare disease", "registry"],
            "hasVersion": "v2",
            "publisherName": "MOLGENIS",
            "publisherDefinition": "Research infrastructure",
            "license": "http://creativecommons.org/licenses/by/4.0/",
            "landingPage": "https://catalogue.example.org/catalogues/C1",
            "language": ["en", "nl"],
            "accessRights": "http://publications.europa.eu/resource/authority/access-right/PUBLIC",
            "hasPolicy": "http://example.org/policies/standard",
            "relation": "http://example.org/relations/related-dataset",
            "contactEmail": "info@molgenis.org",
        }
    ]
    load_raw_from_rows(
        rows,
        raw,
        MappingConfig(raw=RawMapping(entity_name="Dataset", subject_path="catalog")),
    )

    dcat = construct_dcat(raw)
    DCAT = Namespace("http://www.w3.org/ns/dcat#")
    DCT = Namespace("http://purl.org/dc/terms/")
    FOAF = Namespace("http://xmlns.com/foaf/0.1/")
    VCARD = Namespace("http://www.w3.org/2006/vcard/ns#")

    res = EX["catalog/C1"]
    assert (res, RDF.type, DCAT["Dataset"]) in dcat
    assert (res, DCT["title"], None) in dcat
    assert (res, DCT["description"], None) in dcat
    assert (res, DCAT["keyword"], None) in dcat
    assert (res, DCAT["theme"], None) in dcat
    assert (res, DCT["hasVersion"], None) in dcat
    assert (res, DCT["license"], None) in dcat
    assert (res, DCT["language"], None) in dcat
    assert (res, DCT["accessRights"], None) in dcat
    assert (res, DCAT["hasPolicy"], None) in dcat
    assert (res, DCT["relation"], None) in dcat
    assert (res, DCAT["landingPage"], None) in dcat

    publisher = EX["publisher/MOLGENIS"]
    assert (res, DCT["publisher"], publisher) in dcat
    assert (publisher, RDF.type, FOAF["Organization"]) in dcat
    assert (publisher, FOAF["name"], None) in dcat
    assert (publisher, DCT["description"], None) in dcat

    assert (None, RDF.type, VCARD["Individual"]) in dcat


def test_select_rows():
    raw = new_graph()
    rows = [
        {
            "id": "R1",
            "name": "My Resource",
            "description": "Example resource",
            "website": "https://example.org",
            "contactEmail": "foo@org.org",
        },
        {
            "id": "R2",
            "name": "Another Resource",
            "description": "Other resource",
        },
    ]
    load_raw_from_rows(rows, raw, MappingConfig(raw=RawMapping()))

    result = select_rows(raw, "profiles/dcat/sparql/select.sparql")
    assert len(result) == 2
    ids = {row["id"] for row in result}
    assert ids == {"R1", "R2"}


def test_export_profile_load_and_shacl_validation():
    profile = load_profile("dcat", expected_kind="export")
    raw = new_graph()
    rows = [
        {
            "id": "R1",
            "name": "Dataset One",
            "description": "Example description",
        }
    ]
    load_raw_from_rows(rows, raw, profile.mapping)
    dcat = construct_dcat(raw)
    assert profile.shacl is not None
    conforms, report = validate_graph(dcat, profile.shacl)
    assert conforms is True
    assert len(report) >= 0


def test_yaml_mapping_alias_and_iri_coercion():
    mapping_path = Path(__file__).parent / "resources" / "mapping.yml"
    mapping = load_mapping_override(str(mapping_path))
    assert mapping is not None
    raw = new_graph()
    rows = [
        {
            "code": "R9",
            "label": "Aliased Name",
            "website": "https://example.org/resource",
        }
    ]
    load_raw_from_rows(rows, raw, mapping)
    res = URIRef("https://catalogue.org/resource/R9")
    assert (res, FIELD["name"], None) in raw
    assert (res, FIELD["website"], URIRef("https://example.org/resource")) in raw


def test_rml_materialize_csv():
    base = Path(__file__).parent / "resources" / "rml"
    mapping_path = base / "mapping.rml.ttl"
    source_path = base / "data.csv"
    graph = materialize_rml(str(mapping_path), str(source_path))
    res = URIRef("https://example.org/resource/1")
    assert (res, URIRef("https://example.org/name"), None) in graph


def test_stdout_requires_single_target():
    raw = new_graph()
    rows = [{"id": "R1", "name": "Example", "description": "Desc"}]
    load_raw_from_rows(rows, raw, MappingConfig(raw=RawMapping()))
    with pytest.raises(ValueError):
        export_formats(
            raw,
            None,
            "profiles/dcat/sparql/select.sparql",
            "profiles/dcat/sparql/construct.sparql",
            ["json", "csv"],
        )


def test_render_csv_includes_union_of_fields():
    rows = [
        {"id": "R1", "name": "Example"},
        {"id": "R2", "name": "Example 2", "label": "Extra"},
    ]
    content = render_csv(rows)
    reader = csv.DictReader(io.StringIO(content))
    assert reader.fieldnames is not None
    assert "label" in reader.fieldnames
    parsed = list(reader)
    assert parsed[0]["label"] == ""
    assert parsed[1]["label"] == "Extra"


def test_field_paths_flatten_nested():
    mapping = MappingConfig(
        raw=RawMapping(),
        field_paths={
            "leadOrganisationAcronym": "leadOrganisation.acronym",
            "countryNames": "countries[].name",
        },
        drop_nested=True,
    )
    raw = new_graph()
    rows = [
        {
            "id": "R2",
            "name": "Nested Resource",
            "leadOrganisation": {"acronym": "ORG"},
            "countries": [{"name": "NL"}, {"name": "BE"}],
        }
    ]
    load_raw_from_rows(rows, raw, mapping)
    res = URIRef("https://catalogue.org/resource/R2")
    assert (res, FIELD["leadOrganisationAcronym"], None) in raw
    assert (res, FIELD["countryNames"], None) in raw


def test_concept_fields_create_skos_nodes():
    mapping = MappingConfig(
        raw=RawMapping(),
        concept_fields={
            "keywords": ConceptField(
                path="keywords[]",
                predicate="keywordConcept",
                uri_path="ontologyTermURI",
                code_path="code",
                label_path="name",
                lang="en",
            )
        },
    )
    raw = new_graph()
    rows = [
        {
            "id": "R3",
            "name": "Concept Resource",
            "keywords": [
                {
                    "name": "genomics",
                    "code": "GEN",
                    "ontologyTermURI": "http://example.org/terms/genomics",
                }
            ],
        }
    ]
    load_raw_from_rows(rows, raw, mapping)
    res = URIRef("https://catalogue.org/resource/R3")
    concept = URIRef("http://example.org/terms/genomics")
    assert (res, FIELD["keywordConcept"], concept) in raw
    assert (
        concept,
        RDF.type,
        Namespace("http://www.w3.org/2004/02/skos/core#")["Concept"],
    ) in raw
    assert (
        concept,
        Namespace("http://www.w3.org/2004/02/skos/core#")["prefLabel"],
        None,
    ) in raw


def test_node_fields_create_distribution_nodes():
    mapping = MappingConfig(
        raw=RawMapping(),
        node_fields={
            "releases": NodeField(
                path="releases[]",
                predicate="distribution",
                subject_path="distribution",
                id_field="id",
                fields={
                    "version": "releaseVersion",
                    "date": "releaseDate",
                    "description": "releaseDescription",
                },
            )
        },
    )
    raw = new_graph()
    rows = [
        {
            "id": "R4",
            "name": "Release Resource",
            "releases": [{"id": "REL-2", "version": "v2", "date": "2024-01-01"}],
        }
    ]
    load_raw_from_rows(rows, raw, mapping)
    res = URIRef("https://catalogue.org/resource/R4")
    dist = URIRef("https://catalogue.org/distribution/REL-2")
    assert (res, FIELD["distribution"], dist) in raw
    assert (dist, FIELD["releaseVersion"], None) in raw
    assert (dist, FIELD["releaseDate"], None) in raw


def test_export_profiles_reference_existing_queries():
    profiles_dir = (
        Path(__file__).parents[1] / "src" / "schema_bridge" / "resources" / "profiles"
    )
    for profile_path in profiles_dir.glob("**/profile.yml"):
        data = load_yaml(str(profile_path), "schema_bridge.resources")
        if str(data.get("kind", "")).lower() != "export":
            continue
        profile = load_profile(str(profile_path), expected_kind="export")
        if profile.graphql_query:
            load_text(
                resolve_profile_path(
                    profile,
                    profile.graphql_query,
                    "schema_bridge.resources",
                ),
                "schema_bridge.resources",
            )
        if profile.select_query:
            select_query = load_text(
                resolve_profile_path(
                    profile,
                    profile.select_query,
                    "schema_bridge.resources",
                ),
                "schema_bridge.resources",
            )
            new_graph().query(select_query)
        if profile.construct_query:
            construct_query = load_text(
                resolve_profile_path(
                    profile,
                    profile.construct_query,
                    "schema_bridge.resources",
                ),
                "schema_bridge.resources",
            )
            new_graph().query(construct_query)
        if profile.shacl:
            load_text(
                resolve_profile_path(
                    profile,
                    profile.shacl.shapes,
                    "schema_bridge.resources",
                ),
                "schema_bridge.resources",
            )
