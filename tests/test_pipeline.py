import csv
import io
from pathlib import Path
from rdflib import Graph, Namespace, URIRef
from rdflib.namespace import RDF

import pytest
from schema_bridge.pipeline import (
    MappingConfig,
    RawMapping,
    construct_dcat,
    export_formats,
    load_text,
    load_raw_from_rows,
    load_mapping_override,
    load_profile,
    materialize_rml,
    render_csv,
    select_rows,
    validate_graph,
)
from schema_bridge.pipeline.mapping import ConceptField, NodeField

EX = Namespace("https://catalogue.org/")
FIELD = Namespace("https://catalogue.org/field/")
ENTITY = Namespace("https://catalogue.org/entity/")


def test_raw_load_and_dcat_construct():
    raw = Graph()
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
    raw = Graph()
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


def test_dcat_all_attributes_construct():
    profile = load_profile("dcat-all-attributes")
    raw = Graph()
    rows = [
        {
            "id": "R-ALL-1",
            "acronym": "RA1",
            "name": "All Attributes Resource",
            "description": "Resource with comprehensive DCAT fields",
            "homepage": "https://example.org/resources/R-ALL-1",
            "type": [{"name": "catalog", "ontologyTermURI": "http://example.org/types/catalog"}],
            "keywords": [
                {"name": "genomics", "ontologyTermURI": "http://example.org/themes/genomics"}
            ],
            "contact": [{"name": "Support", "email": "support@example.org"}],
            "institution": [{"name": "Example Org"}],
            "conditions": [{"ontologyTermURI": "http://example.org/access/public"}],
            "conditionsDescription": "CC-BY",
            "startYear": 2000,
            "endYear": 2020,
            "releases": [{"id": "REL-1", "version": "v1", "date": "2023-01-01"}],
            "contributors": [
                {
                    "contact": {"name": "Jane Doe", "email": "jane@example.org"},
                    "contributionType": [
                        {"ontologyTermURI": "http://example.org/roles/curator"}
                    ],
                    "contributionDescription": "Curated metadata",
                }
            ],
        }
    ]
    load_raw_from_rows(rows, raw, profile.mapping)

    dcat_all = load_text("sparql/dcat_all_construct.sparql", "schema_bridge.resources")
    graph = raw.query(dcat_all).graph

    DCAT = Namespace("http://www.w3.org/ns/dcat#")
    DCT = Namespace("http://purl.org/dc/terms/")
    VCARD = Namespace("http://www.w3.org/2006/vcard/ns#")
    PROV = Namespace("http://www.w3.org/ns/prov#")

    res = EX["resource/R-ALL-1"]
    assert (res, RDF.type, DCAT["Resource"]) in graph
    assert (res, RDF.type, DCAT["Dataset"]) in graph
    assert (res, DCT["identifier"], None) in graph
    assert (res, DCT["title"], None) in graph
    assert (res, DCAT["keyword"], None) in graph
    assert (res, DCAT["theme"], None) in graph
    assert (res, DCT["accessRights"], None) in graph
    assert (res, DCT["license"], None) in graph

    assert (None, RDF.type, VCARD["Individual"]) in graph
    assert (None, VCARD["hasEmail"], None) in graph

    assert (None, RDF.type, DCAT["Distribution"]) in graph
    assert (None, DCT["issued"], None) in graph

    assert (None, RDF.type, DCT["PeriodOfTime"]) in graph
    assert (None, DCAT["startDate"], None) in graph
    assert (None, DCAT["endDate"], None) in graph

    assert (None, RDF.type, PROV["Attribution"]) in graph
    assert (None, PROV["agent"], None) in graph


def test_select_rows():
    raw = Graph()
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

    result = select_rows(raw, "resources_select.sparql")
    assert len(result) == 2
    ids = {row["id"] for row in result}
    assert ids == {"R1", "R2"}


def test_beacon_biosamples_select():
    raw = Graph()
    rows = [
        {
            "id": "B1",
            "pathologicalState": "healthy",
            "biosampleType": "blood",
            "ageOfIndividualAtCollection": "P45Y",
            "dateOfCollection": "2021-01-12",
        }
    ]
    load_raw_from_rows(
        rows,
        raw,
        MappingConfig(raw=RawMapping(entity_name="Biosample", subject_path="biosample")),
    )

    result = select_rows(raw, "beacon_biosamples_select.sparql")
    assert len(result) == 1
    row = result[0]
    assert row["id"] == "B1"
    assert row["biosampleStatus"] == "healthy"
    assert row["sampleOriginType"] == "blood"
    assert row["collectionMoment"] == "P45Y"
    assert row["collectionDate"] == "2021-01-12"


def test_beacon_datasets_select():
    raw = Graph()
    rows = [
        {
            "id": "D1",
            "name": "Dataset 1",
            "description": "Dataset description",
            "keyword": ["genomics"],
        }
    ]
    load_raw_from_rows(
        rows,
        raw,
        MappingConfig(raw=RawMapping(entity_name="Dataset", subject_path="dataset")),
    )

    result = select_rows(raw, "beacon_select.sparql")
    assert len(result) == 1
    row = result[0]
    assert row["id"] == "D1"
    assert row["name"] == "Dataset 1"
    assert row["desc"] == "Dataset description"
    assert row["keywords"] == "genomics"


def test_beacon_cohorts_select():
    raw = Graph()
    rows = [
        {
            "cohortId": "C100",
            "cohortName": "Cohort 100",
            "cohortType": "population-based",
            "cohortDesign": "prospective",
            "cohortSize": "250",
            "inclusionCriteria_ageRange_start_iso8601duration": "P18Y",
            "inclusionCriteria_ageRange_end_iso8601duration": "P65Y",
            "locations": ["NL"],
            "genders": ["female"],
            "cohortDataTypes": ["genomic"],
        }
    ]
    load_raw_from_rows(
        rows,
        raw,
        MappingConfig(
            raw=RawMapping(entity_name="Cohort", subject_path="cohort", id_field="cohortId")
        ),
    )


def test_profile_load_and_shacl_validation():
    profile = load_profile("dcat")
    raw = Graph()
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
    raw = Graph()
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
    raw = Graph()
    rows = [{"id": "R1", "name": "Example", "description": "Desc"}]
    load_raw_from_rows(rows, raw, MappingConfig(raw=RawMapping()))
    with pytest.raises(ValueError):
        export_formats(raw, None, "resources_select.sparql", "dcat_construct.sparql", ["json", "csv"])


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
    raw = Graph()
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
    raw = Graph()
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
    assert (concept, RDF.type, Namespace("http://www.w3.org/2004/02/skos/core#")["Concept"]) in raw
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
    raw = Graph()
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


def test_catalogue_variables_select():
    mapping = MappingConfig(
        raw=RawMapping(entity_name="Variable", subject_path="variable", id_field="name"),
        field_paths={
            "id": "name",
            "resourceId": "resource.id",
            "resourceName": "resource.name",
            "datasetName": "dataset.name",
            "datasetResourceId": "dataset.resource.id",
            "unitName": "unit.name",
            "repeatUnitName": "repeatUnit.name",
            "formatName": "format.name",
            "mappingSyntaxes": "mappings[].syntax",
            "mappingMatchNames": "mappings[].match.name",
            "mappingSourceIds": "mappings[].source.id",
            "mappingSourceVariableNames": "mappings[].sourceVariables[].name",
            "mappingTargetVariableNames": "mappings[].targetVariable.name",
        },
        drop_nested=True,
    )
    raw = Graph()
    rows = [
        {
            "name": "varA",
            "resource": {"id": "RES1", "name": "Resource 1"},
            "dataset": {"name": "Dataset 1", "resource": {"id": "RES1"}},
            "unit": {"name": "kg"},
            "repeatUnit": {"name": "year"},
            "format": {"name": "integer"},
            "mappings": [
                {
                    "syntax": "sql",
                    "match": {"name": "exact"},
                    "source": {"id": "SRC1"},
                    "sourceVariables": [{"name": "sourceVar1"}, {"name": "sourceVar2"}],
                    "targetVariable": {"name": "targetVar1"},
                }
            ],
        }
    ]
    load_raw_from_rows(rows, raw, mapping)
    result = select_rows(raw, "catalogue_variables_select.sparql")
    assert len(result) == 1
    row = result[0]
    assert row["id"] == "varA"
    assert row["resourceId"] == "RES1"
    assert row["datasetName"] == "Dataset 1"
    assert row["mappingSourceVariableNames"] == "sourceVar1|sourceVar2"


def test_construct_queries_mimic_selects():
    raw = Graph()
    rows = [
        {
            "id": "R1",
            "name": "Resource 1",
            "description": "Desc",
            "website": "https://example.org",
            "contactEmail": "a@example.org",
        }
    ]
    load_raw_from_rows(rows, raw, MappingConfig(raw=RawMapping()))
    resources_construct = load_text("sparql/resources_construct.sparql", "schema_bridge.resources")
    resources_graph = raw.query(resources_construct).graph
    res = URIRef("https://catalogue.org/resource/R1")
    DCT = Namespace("http://purl.org/dc/terms/")
    assert (res, DCT["title"], None) in resources_graph

    raw_ds = Graph()
    ds_rows = [
        {
            "id": "D1",
            "name": "Dataset 1",
            "description": "Dataset desc",
            "keyword": ["k1", "k2"],
        }
    ]
    load_raw_from_rows(ds_rows, raw_ds, MappingConfig(raw=RawMapping(entity_name="Dataset", subject_path="dataset")))
    beacon_construct = load_text("sparql/beacon_construct.sparql", "schema_bridge.resources")
    beacon_graph = raw_ds.query(beacon_construct).graph
    ds = URIRef("https://catalogue.org/dataset/D1")
    DCAT = Namespace("http://www.w3.org/ns/dcat#")
    assert (ds, DCT["title"], None) in beacon_graph
    assert (ds, DCAT["keyword"], None) in beacon_graph

    raw_bio = Graph()
    bio_rows = [
        {
            "id": "B1",
            "pathologicalState": "healthy",
            "biosampleType": "blood",
        }
    ]
    load_raw_from_rows(
        bio_rows,
        raw_bio,
        MappingConfig(raw=RawMapping(entity_name="Biosample", subject_path="biosample")),
    )
    biosamples_construct = load_text(
        "sparql/beacon_biosamples_construct.sparql",
        "schema_bridge.resources",
    )
    biosamples_graph = raw_bio.query(biosamples_construct).graph
    bio = URIRef("https://catalogue.org/biosample/B1")
    BIOS = Namespace("https://bioschemas.org/")
    SCHEMA = Namespace("https://schema.org/")
    assert (bio, RDF.type, BIOS["BioSample"]) in biosamples_graph
    assert (bio, SCHEMA["sampleType"], None) in biosamples_graph

    raw_cohort = Graph()
    cohort_rows = [
        {
            "cohortId": "C1",
            "cohortName": "Cohort 1",
            "locations": ["NL"],
        }
    ]
    load_raw_from_rows(
        cohort_rows,
        raw_cohort,
        MappingConfig(raw=RawMapping(entity_name="Cohort", subject_path="cohort", id_field="cohortId")),
    )
    cohorts_construct = load_text(
        "sparql/beacon_cohorts_construct.sparql",
        "schema_bridge.resources",
    )
    cohorts_graph = raw_cohort.query(cohorts_construct).graph
    cohort = URIRef("https://catalogue.org/cohort/C1")
    assert (cohort, RDF.type, Namespace("https://schema.org/")["MedicalObservationalStudy"]) in cohorts_graph
    assert (cohort, Namespace("https://schema.org/")["name"], None) in cohorts_graph

    raw_var = Graph()
    var_rows = [
        {
            "name": "var1",
            "resource": {"id": "RES1"},
            "dataset": {"name": "Dataset 1", "resource": {"id": "RES1"}},
        }
    ]
    load_raw_from_rows(
        var_rows,
        raw_var,
        MappingConfig(
            raw=RawMapping(entity_name="Variable", subject_path="variable", id_field="name"),
            field_paths={
                "id": "name",
                "resourceId": "resource.id",
                "datasetName": "dataset.name",
                "datasetResourceId": "dataset.resource.id",
            },
            drop_nested=True,
        ),
    )
    variables_construct = load_text(
        "sparql/catalogue_variables_construct.sparql",
        "schema_bridge.resources",
    )
    variables_graph = raw_var.query(variables_construct).graph
    var = URIRef("https://catalogue.org/variable/var1")
    SCHEMA = Namespace("https://schema.org/")
    assert (var, RDF.type, SCHEMA["PropertyValue"]) in variables_graph
    assert (var, SCHEMA["propertyID"], None) in variables_graph


def test_profiles_reference_existing_queries():
    profiles_dir = Path(__file__).parents[1] / "src" / "schema_bridge" / "resources" / "profiles"
    for profile_path in profiles_dir.glob("*.yml"):
        profile = load_profile(str(profile_path))
        if profile.graphql_query:
            load_text(
                profile.graphql_query
                if "/" in profile.graphql_query
                else f"graphql/{profile.graphql_query}",
                "schema_bridge.resources",
            )
        if profile.select_query:
            select_query = load_text(
                profile.select_query
                if "/" in profile.select_query
                else f"sparql/{profile.select_query}",
                "schema_bridge.resources",
            )
            Graph().query(select_query)
        if profile.construct_query:
            construct_query = load_text(
                profile.construct_query
                if "/" in profile.construct_query
                else f"sparql/{profile.construct_query}",
                "schema_bridge.resources",
            )
            Graph().query(construct_query)
        if profile.shacl:
            load_text(
                profile.shacl.shapes
                if "/" in profile.shacl.shapes
                else f"shacl/{profile.shacl.shapes}",
                "schema_bridge.resources",
            )
