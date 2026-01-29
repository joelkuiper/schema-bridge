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
You can pass a profile name, a profile folder, or a direct path to `profile.yml`.

- `uv run schema-bridge run --profile dcat --format ttl`
- `uv run schema-bridge run --profile fdp --format ttl`
- `uv run schema-bridge run --profile health-dcat-ap-molgenis --format ttl`

Add `--debug` to any command to enable verbose logging (both `schema-bridge` and `schema-bridge-ingest`). It can be
placed before or after the subcommand.
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

- `fetch.graphql`: GraphQL file (relative to the profile folder)
- `fetch.root_key`: GraphQL data root
- `export.select` / `export.construct`: SPARQL files (relative to the profile folder)
- `validate.shacl`: shape path (relative to the profile folder, or an absolute path)
- `validate.enabled`: enable/disable validation

Profiles live under `src/schema_bridge/resources/profiles/<profile>/profile.yml`.

Available export profiles in this repo:

- `dcat`
- `fdp`
- `health-dcat-ap-molgenis`

## MOLGENIS Catalogue (Health-DCAT-AP)

For molgeniscatalogue.org, use the `health-dcat-ap-molgenis` profile. It uses the live schema at
`https://molgeniscatalogue.org/catalogue/graphql`, but the construct is simplified.

To export from molgeniscatalogue.org, point `--base-url` to `https://molgeniscatalogue.org/` and `--schema` to `catalogue`.

Caveats (demo profile):

- The construct is limited and does not emit every optional Health-DCAT-AP enrichment.
- Fields like detailed age-group hierarchy, inclusion criteria details, and design publications are not fully materialized.
- Access-rights and policy nodes are derived from URI fields only; vocabulary labels/definitions may be omitted.

## Output formats

Use `--format` to choose a single output format (`csv`, `json`, `jsonld`, `ttl`, `rdfxml`, `nt`). Export commands write to stdout, so
redirect to a file when you need a saved artifact.

## Resource tree

```
src/schema_bridge/resources/
  profiles/         # export profile folders (profile.yml + graphql/sparql, SHACL shapes)
  ingest_profiles/  # ingest profile folders (profile.yml + graphq/sparql)
  rml/              # RML mappings (optional)
```

## Concept-aware mappings

Profiles can define `concept_fields` to preserve ontology structure (URI/code/label) as SKOS concepts
instead of flattening to strings. This is important for DUO/ICD/SNOMED and similar vocabularies in
biobank and longitudinal study catalogues.
