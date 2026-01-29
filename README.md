# Schema Bridge

Schema Bridge is a small, standalone pipeline for turning EMX2 data into standard metadata formats (DCAT, DCAT-AP,
Health-DCAT, FDP), and for doing the reverse: ingesting RDF metadata back into EMX2.

It exists to make export and interchange easy, and optionally offline, without needing the full server-side RDF
stack or Beacon services.

## What it does

- Fetch EMX2 rows via GraphQL.
- Build a canonical RDF graph from those rows.
- Export RDF (TTL/JSON-LD/RDF/XML/N-Triples) and tabular formats via SPARQL queries.
- Validate outputs with SHACL (optional, on by default).
- (Optionally) Ingest RDF (TTL/JSON-LD/RDF/XML/N-Triples) into EMX2 via GraphQL mutations.

## Quick start (export)

Install dependencies: `uv sync --extra test`.

Profiles are the primary entry point. A profile wires together the fetch, export, mapping, and validation steps.

- `uv run schema-bridge run --profile dcat --format ttl`
- `uv run schema-bridge run --profile dcat-all-attributes --format ttl`
- `uv run schema-bridge run --profile dcat-ap-3.0.1 --format ttl`
- `uv run schema-bridge run --profile fdp --format ttl`
- `uv run schema-bridge run --profile health-dcat-ap --format ttl`
- `uv run schema-bridge run --profile health-ri-core-v2 --format ttl`

You can set a limit (`--limit`) to adjust the number of GraphQL results to be fetched. For large
catalogues, `--page-size` enables server-side paging, and `--updated-since`/`--updated-until` allow
incremental syncs based on the `mg_updatedOn` system column.

## Profiles (export)

Profiles are YAML files with explicit sections:

- `fetch`: GraphQL query + root key
- `export`: SPARQL select/construct
- `mapping`: how rows are flattened into the canonical graph
- `validate`: SHACL shapes + toggle

Common keys:

- `fetch.graphql`: GraphQL file (under `resources/graphql/`)
- `fetch.root_key`: GraphQL data root
- `export.select` / `export.construct`: SPARQL files (under `resources/sparql/`)
- `validate.shacl`: shape path (under `resources/shacl/`)
- `validate.enabled`: enable/disable validation

Profiles live under `src/schema_bridge/resources/profiles/`.

## CLI flags (pagination + incremental sync)

The `fetch` and `run` commands support paging and updated-since filters:

- `--page-size` rows per GraphQL page (default: 200)
- `--limit` max rows to fetch (`0` means all rows)
- `--updated-since` ISO-8601 timestamp to filter on `mg_updatedOn` (inclusive)
- `--updated-until` ISO-8601 timestamp to cap the range
Note: `--updated-since/--updated-until` require `mg_updatedOn` filter support in the server schema.

## DCAT all-attributes use case (catalogue schema)

The `dcat-all-attributes` profile is a best-effort, comprehensive mapping that attempts to populate every
property shown in the [DCAT “all attributes” diagram](https://www.w3.org/TR/vocab-dcat/images/dcat-all-attributes.svg). It flattens nested EMX2 catalogue fields and emits
`dcat:Resource`, `dcat:Dataset`, `dcat:Catalog`, `dcat:DataService`, `dcat:Distribution`, `dcat:CatalogRecord`,
`dcat:Relationship`, and `dcat:DatasetSeries` where possible.

Example usage:

- `uv run schema-bridge run --profile dcat-all-attributes --format ttl`

Caveats (current catalogue schema limitations):

- Distribution/service fields like `downloadURL`, `accessURL`, `mediaType`, `byteSize`, `checksum`,
  `endpointURL`, and `endpointDescription` are not present in the default EMX2 catalogue schema.
- Releases are flattened when exported from `Resources`, so per-release fields (version/date/description)
  may not align with each distribution node unless you model Releases separately.
- Some class assignments (Catalog/DataService/DatasetSeries) are inferred from type names and are heuristic.

## Ingest (RDF -> EMX2 via GraphQL)

Use ingest when you already have DCAT/Health-DCAT RDF (TTL/JSON-LD/RDF/XML/N-Triples) and want to push it into EMX2. Validation is
enabled by default.

Ingest profiles are smaller and only describe ingest steps. They live under
`src/schema_bridge/resources/ingest_profiles/`.

Example ingest profile:

```yaml
name: ingest-dcat
validate:
  shacl: shacl/dcat.shacl.ttl
  enabled: true
extract:
  sparql: sparql/ingest_dcat_select.sparql
upload:
  table: Resource
  mode: upsert
  id_prefix: import-
  batch_size: 100
graphql:
  base_url: https://emx2.dev.molgenis.org/
  schema: catalogue-demo
  token: YOUR_TOKEN
  graphql_mutation: graphql/ingest_mutation.graphql
```

Run ingest with a profile:

- `uv run schema-bridge-ingest ingest path/to/catalogue.ttl --profile ingest-dcat`

Override with flags (flags take precedence):

- `uv run schema-bridge-ingest ingest path/to/catalogue.ttl --profile ingest-dcat --schema catalogue-demo --no-validate`

## Fetch (advanced)

Profiles are the intended route. Use `fetch` only when you want to inspect or pin the raw GraphQL response for offline
or CI use.

- `uv run schema-bridge fetch --base-url https://emx2.dev.molgenis.org/ --schema catalogue-demo --limit 5 -o out/graphql.json`
- `uv run schema-bridge fetch --base-url https://emx2.dev.molgenis.org/ --schema catalogue-demo --page-size 200 --limit 0 -o out/graphql.json`

## Convert (advanced)

Convert a previously fetched GraphQL JSON file:

- `uv run schema-bridge convert out/graphql.json --profile dcat --format ttl`

## Output formats

Use `--format` to choose a single output format (`csv`, `json`, `jsonld`, `ttl`, `rdfxml`, `nt`). Export commands write to stdout, so
redirect to a file when you need a saved artifact.

Example: `uv run schema-bridge run --profile dcat --format ttl > out/resources.ttl`

## Resource tree

```
src/schema_bridge/resources/
  graphql/          # GraphQL queries
  sparql/           # SPARQL SELECT/CONSTRUCT queries
  profiles/         # export profiles (YAML)
  ingest_profiles/  # ingest profiles (YAML)
  shacl/            # SHACL shapes
  rml/              # RML mappings (optional)

## Concept-aware mappings

Profiles can define `concept_fields` to preserve ontology structure (URI/code/label) as SKOS concepts
instead of flattening to strings. This is important for DUO/ICD/SNOMED and similar vocabularies in
biobank and longitudinal study catalogues. The `dcat-all-attributes` profile now emits richer
concept nodes and links releases as explicit `dcat:Distribution` resources.
```

## Stdout output

`run` and `convert` always write a single format to stdout. Pick one with `--format`.

Example: JSON to stdout (truncated):

- `uv run schema-bridge convert out/graphql.json --profile dcat --format json`

```json
{
  "rows": [
    {
      "id": "ATHLETE",
      "name": "Advancing Tools for Human Early Lifecourse Exposome Research and Translation",
      "desc": "Children are particularly vulnerable to environmental hazards. The ATHLETE project will measure a wide range of environmental exposures (urban, chemical, lifestyle and social risk factors) during pregnancy, childhood, and adolescence. The project will then link this “early-life exposome” to children’s biological responses and cardiometabolic, respiratory, and mental health. The results will help us to better understand and prevent health damage from environmental agents from the earliest parts of the life course onward.",
      "keywords": ""
    }
  ]
}
```

Example: DCAT Turtle to stdout (truncated):

- `uv run schema-bridge convert out/graphql.json --profile dcat --no-validate --format ttl`

```turtle
@prefix dcat: <http://www.w3.org/ns/dcat#> .
@prefix dcterms: <http://purl.org/dc/terms/> .

<https://catalogue.org/resource/ATHLETE> a dcat:Dataset ;
    dcterms:description "Children are particularly vulnerable to environmental hazards. The ATHLETE project will measure a wide range of environmental exposures (urban, chemical, lifestyle and social risk factors) during pregnancy, childhood, and adolescence. The project will then link this “early-life exposome” to children’s biological responses and cardiometabolic, respiratory, and mental health. The results will help us to better understand and prevent health damage from environmental agents from the earliest parts of the life course onward." ;
    dcterms:identifier "ATHLETE" ;
    dcterms:title "Advancing Tools for Human Early Lifecourse Exposome Research and Translation" ;
    dcat:landingPage <https://athleteproject.eu/> .
```

## Examples (captured output)

The following examples are generated from the bundled test fixtures to keep them reproducible.

### DCAT all-attributes (Turtle)

Command:

```bash
uv run schema-bridge run --profile dcat-all-attributes --format ttl
```

Output (truncated):

```turtle
@prefix dcat: <http://www.w3.org/ns/dcat#> .
@prefix dcterms: <http://purl.org/dc/terms/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix ns1: <http://www.w3.org/2006/vcard/ns#> .
@prefix prov: <http://www.w3.org/ns/prov#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://catalogue.org/contact/support%40example.org> a ns1:Individual ;
    ns1:fn "Support" ;
    ns1:hasEmail <mailto:support@example.org> .

<https://catalogue.org/distribution/REL-1> a dcat:Distribution ;
    dcterms:accessRights <http://example.org/access/public> ;
    dcterms:conformsTo <http://example.org/criteria/adult> ;
    dcterms:description "Initial release" ;
    dcterms:issued "2023-01-01" ;
    dcterms:license "CC-BY" ;
    dcterms:title "v1" ;
    dcat:accessURL <https://example.org/docs/data-dictionary> ;
    dcat:version "v1" .

<https://catalogue.org/resource/R-DCAT-1> a dcat:Catalog,
        dcat:Dataset,
        dcat:Resource ;
    dcterms:accessRights <http://example.org/access/public> ;
    dcterms:conformsTo <http://example.org/criteria/adult> ;
    dcterms:description "Resource with rich DCAT metadata" ;
    dcterms:identifier "RDC1" ;
    dcterms:title "Comprehensive Resource" ;
    dcat:distribution <https://catalogue.org/distribution/REL-1> ;
    dcat:keyword "genomics" ;
    dcat:landingPage <https://example.org/resources/R-DCAT-1> ;
    dcat:theme <http://example.org/themes/genomics> ;
    dcat:version "v1" .
```

### Health-DCAT-AP (Turtle)

Command:

```bash
uv run schema-bridge run --profile health-dcat-ap --format ttl
```

Output (truncated):

```turtle
@prefix dcat: <http://www.w3.org/ns/dcat#> .
@prefix dcterms: <http://purl.org/dc/terms/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix ns1: <http://www.w3.org/2006/vcard/ns#> .
@prefix odrl: <http://www.w3.org/ns/odrl/2/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://catalogue.org/resource/H1> a dcat:Dataset ;
    dcterms:accessRights <http://example.org/dac> ;
    dcterms:conformsTo <https://catalogue.org/criteria/IC1> ;
    dcterms:description "Health DCAT-AP example" ;
    dcterms:identifier "H1" ;
    dcterms:publisher <https://catalogue.org/org/HLTH> ;
    dcterms:rights "Access fee: 0" ;
    dcterms:spatial <https://catalogue.org/geo/EU> ;
    dcterms:temporal [ a dcterms:PeriodOfTime ;
            dcat:endDate "2020"^^xsd:gYear ;
            dcat:startDate "2001"^^xsd:gYear ] ;
    dcterms:title "Health Dataset" ;
    dcat:contactPoint <https://catalogue.org/contact/contact%40health.example.org> ;
    dcat:keyword "Population" ;
    dcat:landingPage <https://health.example.org> ;
    dcat:theme <https://catalogue.org/theme/Population> ;
    odrl:hasPolicy <http://example.org/duc> .
```
