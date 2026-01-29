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
- `uv run schema-bridge run --profile fdp --format ttl`
- `uv run schema-bridge run --profile health-dcat-ap-molgenis --format ttl`

Add `--debug` to any command to enable verbose logging (both `schema-bridge` and `schema-bridge-ingest`).
Use e.g. `uv run schema-bridge run --help` for detailed help.

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

Available export profiles in this repo:

- `dcat`
- `fdp`
- `health-dcat-ap-molgenis`

## MOLGENIS Catalogue (Health-DCAT-AP)

For molgeniscatalogue.org, use the `health-dcat-ap-molgenis` profile. It includes a
maximum-coverage GraphQL query aligned to the live schema at `https://molgeniscatalogue.org/catalogue/graphql`.

To export from molgeniscatalogue.org, point `--base-url` to `https://molgeniscatalogue.org/` and `--schema` to `catalogue`.

## CLI flags (pagination + incremental sync)

The `fetch` and `run` commands support paging and updated-since filters:

- `--page-size` rows per GraphQL page (default: 200)
- `--limit` max rows to fetch (`0` means all rows)
- `--updated-since` ISO-8601 timestamp to filter on `mg_updatedOn` (inclusive)
- `--updated-until` ISO-8601 timestamp to cap the range
Note: `--updated-since/--updated-until` require `mg_updatedOn` filter support in the server schema.

## Ingest (RDF -> EMX2 via GraphQL)

Use ingest when you already have DCAT/Health-DCAT RDF (TTL/JSON-LD/RDF/XML/N-Triples) and want to push it into EMX2. Validation is
enabled by default.

Ingest profiles live under `src/schema_bridge/resources/ingest_profiles/`.

## Output formats

Use `--format` to choose a single output format (`csv`, `json`, `jsonld`, `ttl`, `rdfxml`, `nt`). Export commands write to stdout, so
redirect to a file when you need a saved artifact.

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

## Concept-aware mappings

Profiles can define `concept_fields` to preserve ontology structure (URI/code/label) as SKOS concepts
instead of flattening to strings. This is important for DUO/ICD/SNOMED and similar vocabularies in
biobank and longitudinal study catalogues.
