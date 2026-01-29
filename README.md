# Schema Bridge

Schema Bridge is a small, standalone pipeline for turning EMX2 data into standard metadata formats (DCAT, DCAT-AP,
Health-DCAT, FDP), and for doing the reverse: ingesting RDF metadata back into EMX2.

It exists to make export and interchange easy, and optionally offline, without needing the full server-side RDF
stack or Beacon services.

## What it does

- Fetch EMX2 rows via GraphQL.
- Build a canonical RDF graph from those rows.
- Export RDF (TTL/JSON-LD/RDF/XML/N-Triples) and tabular formats via SPARQL queries.
- Validate outputs with SHACL (optional).
- Or, optionally, ingest RDF (TTL/JSON-LD/RDF/XML/N-Triples) into EMX2 via GraphQL mutations.

## Quick start (export)

Install dependencies: `uv sync --extra test`.

Profiles ([profile directory](./src/schema_bridge/resources/profiles)] are the primary entry point. Export profiles (with `kind: export`) wire together fetch, export, mapping,
and validation steps. You can pass a profile name, a profile folder, or a direct path to `profile.yml`.

- `uv run schema-bridge export --profile dcat --format ttl`
- `uv run schema-bridge export --profile fdp --format ttl`
- `uv run schema-bridge export --profile health-dcat-ap-molgenis --format ttl`

Add `--debug` to any command to enable verbose logging. It can be placed before or after the subcommand.

You can set a limit (`--limit`) to adjust the number of GraphQL results to be fetched.
Use the command-line help, e.g. `uv run schema-bridge export --help`, for detailed information about parameters.

### GraphQL endpoint resolution (export)

For `fetch`/`export`, the GraphQL endpoint is resolved in this order (highest wins):

1. CLI `--graphql-endpoint`
2. Profile `fetch.endpoint`
3. Environment `SCHEMA_BRIDGE_GRAPHQL_ENDPOINT`
4. CLI `--base-url` + `--schema`
5. Profile `fetch.base_url` + `fetch.schema`
6. Environment `SCHEMA_BRIDGE_BASE_URL` + `SCHEMA_BRIDGE_SCHEMA`
7. No defaults are assumed; if nothing is provided, the CLI errors.

If an endpoint is provided, `base_url/schema` are ignored. Otherwise they are combined as
`{base_url}/{schema}/graphql`.

## Profiles

All profiles live under `src/schema_bridge/resources/profiles/<profile>/profile.yml` and include a `kind` field
(`export` or `ingest`) to indicate which pipeline they drive.

### Export profiles

Export profiles are YAML files with explicit sections:

- `fetch`: GraphQL query + root key
- `export`: SPARQL select/construct
- `mapping`: how rows are flattened into the canonical graph
- `validate`: SHACL shapes + toggle

Common keys:

- `fetch.graphql`: GraphQL file (relative to the profile folder)
- `fetch.endpoint`: full GraphQL endpoint URL (optional)
- `fetch.base_url` + `fetch.schema`: base URL + schema name (optional)
- `fetch.root_key`: GraphQL data root
- `export.select` / `export.construct`: SPARQL files (relative to the profile folder)
- `validate.shacl`: shape path (relative to the profile folder, or an absolute path)
- `validate.enabled`: enable/disable validation

Available export profiles in this repo:

- `dcat`
- `fdp`
- `health-dcat-ap-molgenis`

## MOLGENIS Catalogue (Health-DCAT-AP)

For molgeniscatalogue.org, use the `health-dcat-ap-molgenis` profile.

Caveats (demo profile):

- The construct is limited and does not emit every optional Health-DCAT-AP enrichment.
- Fields like detailed age-group hierarchy, inclusion criteria details, and design publications are not fully materialized.
- Access-rights and policy nodes are derived from URI fields only; vocabulary labels/definitions may be omitted.

## Output formats

Use `--format` to choose a single output format (`csv`, `json`, `jsonld`, `ttl`, `rdfxml`, `nt`). Export commands write to stdout, so
redirect to a file when you need a saved artifact.

## Quick start (ingest)

Ingest profiles (with `kind: ingest`) control how RDF gets converted into rows and uploaded via GraphQL mutations.
Use `--profile` to select a packaged profile, profile folder, or `profile.yml`.

- `uv run schema-bridge ingest path/to/input.ttl --profile ingest-dcat --base-url https://emx2.dev.molgenis.org/ --schema catalogue-demo --dry-run`
- `uv run schema-bridge ingest path/to/input.ttl --profile ingest-dcat --out out/rows.json --dry-run`

`--format` is optional; if omitted, the RDF format is inferred from the input file extension.
Use `--dry-run` or `--out` to inspect the generated rows without uploading them.
GraphQL targets are resolved in this order: CLI `--base-url/--schema`, ingest profile `graphql.base_url/graphql.schema`,
environment (`SCHEMA_BRIDGE_BASE_URL`/`SCHEMA_BRIDGE_SCHEMA`), and finally the built-in defaults.

### Ingest profiles

Ingest profiles are YAML files with explicit sections:

- `validate`: SHACL shapes + toggle
- `extract`: SPARQL select query used to extract rows from RDF
- `upload`: target table + mutation settings
- `graphql`: base URL/schema defaults (optional)

Common keys:

- `validate.shacl`: shape path (relative to the profile folder, or an absolute path)
- `validate.enabled`: enable/disable validation
- `extract.sparql`: SPARQL SELECT file (relative to the profile folder)
- `upload.table`: target EMX2 table name
- `upload.mode`: mutation mode (`upsert` or `insert`)
- `upload.id_prefix`: prefix for generated EMX2 ids
- `upload.batch_size`: rows per mutation batch
- `graphql.base_url` + `graphql.schema`: default GraphQL location (optional)
- `graphql.token`: bearer token for auth (optional)
