# Schema Bridge

Schema Bridge is a small, standalone pipeline for turning EMX2 data into standard metadata formats (DCAT, DCAT-AP,
Health-DCAT, FDP), and for doing the reverse: ingesting RDF metadata back into EMX2.

It exists to make export and interchange easy, and optionally offline, without needing the full server-side RDF
stack or Beacon services.

## What it does

- Fetch EMX2 rows via GraphQL.
- Build a canonical RDF graph from those rows.
- Export RDF (TTL/JSONLD) and tabular formats via SPARQL queries.
- Validate outputs with SHACL (optional, on by default).
- (Optionally) Ingest RDF (TTL/JSON-LD) into EMX2 via GraphQL mutations.

## Quick start (export)

Install dependencies: `uv sync --extra test`.

Profiles are the primary entry point. A profile wires together the fetch, export, mapping, and validation steps.

- `uv run schema-bridge run --profile dcat -o out`
- `uv run schema-bridge run --profile dcat-all-attributes -o out`
- `uv run schema-bridge run --profile dcat-ap-3.0.1 -o out`
- `uv run schema-bridge run --profile fdp -o out`
- `uv run schema-bridge run --profile health-dcat-ap -o out`
- `uv run schema-bridge run --profile health-ri-core-v2 -o out`

## Profiles (export)

Profiles are YAML files with explicit sections:

- `fetch`: GraphQL query + root key
- `export`: SPARQL select/construct + outputs
- `mapping`: how rows are flattened into the canonical graph
- `validate`: SHACL shapes + toggle

Common keys:

- `fetch.graphql`: GraphQL file (under `resources/graphql/`)
- `fetch.root_key`: GraphQL data root
- `export.select` / `export.construct`: SPARQL files (under `resources/sparql/`)
- `export.outputs`: formats to emit (`csv`, `json`, `jsonld`, `ttl`)
- `validate.shacl`: shape path (under `resources/shacl/`)
- `validate.enabled`: enable/disable validation

Profiles live under `src/schema_bridge/resources/profiles/`.

## DCAT all-attributes use case (catalogue schema)

The `dcat-all-attributes` profile is a best-effort, comprehensive mapping that attempts to populate every
property shown in the [DCAT “all attributes” diagram](https://www.w3.org/TR/vocab-dcat/images/dcat-all-attributes.svg). It flattens nested EMX2 catalogue fields and emits
`dcat:Resource`, `dcat:Dataset`, `dcat:Catalog`, `dcat:DataService`, `dcat:Distribution`, `dcat:CatalogRecord`,
`dcat:Relationship`, and `dcat:DatasetSeries` where possible.

Example usage:

- `uv run schema-bridge run --profile dcat-all-attributes -o out`

Caveats (current catalogue schema limitations):

- Distribution/service fields like `downloadURL`, `accessURL`, `mediaType`, `byteSize`, `checksum`,
  `endpointURL`, and `endpointDescription` are not present in the default EMX2 catalogue schema.
- Releases are flattened when exported from `Resources`, so per-release fields (version/date/description)
  may not align with each distribution node unless you model Releases separately.
- Some class assignments (Catalog/DataService/DatasetSeries) are inferred from type names and are heuristic.

## Ingest (RDF -> EMX2 via GraphQL)

Use ingest when you already have DCAT/Health-DCAT RDF (TTL/JSON-LD) and want to push it into EMX2. Validation is
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

## Convert (advanced)

Convert a previously fetched GraphQL JSON file:

- `uv run schema-bridge convert out/graphql.json --profile dcat -o out`

## Output formats

By default, DCAT profiles write:

- `out/resources.csv`
- `out/resources.json`
- `out/resources.jsonld`
- `out/resources.ttl`

The fetch/run commands also write:

- `out/graphql.json`

## Resource tree

```
src/schema_bridge/resources/
  graphql/          # GraphQL queries
  sparql/           # SPARQL SELECT/CONSTRUCT queries
  profiles/         # export profiles (YAML)
  ingest_profiles/  # ingest profiles (YAML)
  shacl/            # SHACL shapes
  rml/              # RML mappings (optional)
```

## Stdout output

When `--out-dir` is omitted, `convert` writes the selected format to stdout.
Use exactly one target format.

Example: JSON to stdout (truncated):

- `uv run schema-bridge convert out/graphql.json --profile dcat -t json`

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

- `uv run schema-bridge convert out/graphql.json --profile dcat --no-validate -t ttl`

```turtle
@prefix dcat: <http://www.w3.org/ns/dcat#> .
@prefix dcterms: <http://purl.org/dc/terms/> .

<https://catalogue.org/resource/ATHLETE> a dcat:Dataset ;
    dcterms:description "Children are particularly vulnerable to environmental hazards. The ATHLETE project will measure a wide range of environmental exposures (urban, chemical, lifestyle and social risk factors) during pregnancy, childhood, and adolescence. The project will then link this “early-life exposome” to children’s biological responses and cardiometabolic, respiratory, and mental health. The results will help us to better understand and prevent health damage from environmental agents from the earliest parts of the life course onward." ;
    dcterms:identifier "ATHLETE" ;
    dcterms:title "Advancing Tools for Human Early Lifecourse Exposome Research and Translation" ;
    dcat:landingPage <https://athleteproject.eu/> .
```
