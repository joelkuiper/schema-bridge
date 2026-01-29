# Schema Bridge

Schema Bridge is a small, standalone pipeline for transforming **GraphQL-shaped data** into RDF
(DCAT, DCAT-AP, Health-DCAT-AP, FDP) and for ingesting RDF metadata back into a GraphQL backend.

More abstractly, Schema Bridge solves the problem of "bidirectional interchange between concrete API-shaped data and standardised metadata representations", without hard-coding schemas or mappings into application logic.

It is **profile-driven**: the same pipeline can target different GraphQL schemas, mapping conventions, and output standards without code changes.

Core implementation locations:

* CLI entrypoint: `src/schema_bridge/cli.py`
* Profile loading and resolution: `src/schema_bridge/pipeline/profiles.py`
* Packaged profiles: `src/schema_bridge/resources/profiles/`

---

## Example: MOLGENIS Catalogue → Health-DCAT-AP

A concrete use case is exporting the **MOLGENIS Catalogue** to **Health-DCAT-AP Release 5**, as used by
[molgeniscatalogue.org](https://molgeniscatalogue.org).

Schema Bridge provides a packaged profile, `health-dcat-ap-molgenis`, which maps catalogue data exposed via GraphQL into a Health-DCAT-AP–compatible RDF representation.

This profile emits:

* A `dcat:Catalog` containing datasets and catalog records
* A `dcat:CatalogRecord` for each dataset
* A `dcat:Dataset` with landing page, publisher, contact point, spatial and temporal coverage, and themes/keywords
* One or more `dcat:Distribution` resources per dataset, including access URLs and access-rights or policy links when available
* Health-DCAT-AP extensions for population coverage and counts when corresponding fields are present
* Access-rights, themes, and population coverage represented as `skos:Concept` nodes when URIs are available

Example usage:

```bash
uv run schema-bridge export \
  --profile health-dcat-ap-molgenis \
  --format rdfxml
```

```bash
uv run schema-bridge export \
  --profile health-dcat-ap-molgenis \
  --format jsonld \
  --limit 10
```

Notes on scope:

* The profile reflects the fields currently exposed by the catalogue GraphQL schema
* Coverage of optional Health-DCAT-AP constructs is partial
* Some nodes are derived from identifiers alone and may lack full lexical annotation
* The mapping should be read as **provisional** and **incomplete**, intended to be extended as source data or requirements evolve

This section is illustrative: the same pipeline can target other domains, schemas, or metadata standards by swapping profiles.

---

## Conceptual overview

Schema Bridge implements **two symmetric pipelines**:

1. **Export**: GraphQL → canonical RDF → serialized outputs
2. **Ingest**: RDF → extracted rows → GraphQL mutations

Both pipelines are configured entirely through **profiles**.

---

## What Schema Bridge does

### Common building blocks

* GraphQL as the source or target interface
* A canonical RDF graph as the interchange layer
* SPARQL for projection (SELECT / CONSTRUCT)
* SHACL for optional validation

### Export pipeline (GraphQL → RDF)

* Fetch rows via GraphQL
* Build a canonical RDF graph
* Export:

  * RDF: TTL, JSON-LD, RDF/XML, N-Triples
  * Tabular formats via SPARQL SELECT
* Optionally validate with SHACL

### Ingest pipeline (RDF → GraphQL)

* Read RDF metadata (TTL / JSON-LD / RDF/XML / N-Triples)
* Extract rows via SPARQL SELECT
* Optionally validate with SHACL
* Upload rows via GraphQL mutations

---

## Installation

```bash
uv sync --extra test
```

---

## Profiles (shared concept)

Profiles are the **primary entry point** to Schema Bridge.

All profiles live under:

```
src/schema_bridge/resources/profiles/<profile>/profile.yml
```

Each profile declares a `kind`:

* `export` → drives the export pipeline
* `ingest` → drives the ingest pipeline

A profile can be referenced by:

* profile name
* profile directory
* direct path to `profile.yml`

---

## Why a canonical RDF layer?

### TL;DR

Schema Bridge uses RDF as a canonical intermediate representation because it provides a stable, schema-flexible graph model for heterogeneous data. GraphQL results are lifted into a canonical graph, transformed and optionally validated declaratively, then serialized or re-materialized. RDF is used strictly as an interchange layer; no persistent triple store, reasoning engine, or ontology commitment is assumed.

### Explanation

At its core, RDF provides a minimal way of expressing relationships between entities. Information is represented as triples consisting of a subject, a predicate, and an object. Each element is identified by a URI, which allows relationships to be named explicitly rather than implied by structure. Taken together, these triples form a directed, labeled graph.

This representation has several practical consequences. Schemas can evolve without invalidating existing data, because new predicates can be introduced alongside existing ones. Data originating from independent systems can be merged without prior coordination, because shared identifiers act as natural join points. Partial data is representable without artificial placeholders, and absence of information does not require schema changes.

These properties align closely with the realities of metadata transformation. GraphQL APIs tend to expose application-specific structures, while metadata standards impose different conceptual models. A canonical graph representation provides a neutral middle layer in which these differences can be reconciled explicitly.

Schema Bridge does not treat RDF as a storage layer or application database. Instead, RDF functions as a temporary, inspectable representation that sits between concrete systems:

```
GraphQL rows  →  canonical RDF graph  →  standard exports
standard RDF →  extracted rows       →  GraphQL mutations
```

This keeps mappings declarative, validation orthogonal, and export and ingest conceptually symmetric. RDF is used where it simplifies transformation and inspection, and discarded once the transformation is complete.

---

# Export pipeline

## Quick start (export)

Export profiles wire together **fetch → mapping → export → validation**.

```bash
uv run schema-bridge export --profile dcat --format ttl
uv run schema-bridge export --profile fdp --format ttl
uv run schema-bridge export --profile health-dcat-ap-molgenis --format ttl
```

Useful options:

* `--format` — output format
* `--limit` — limit GraphQL rows fetched
* `--debug` — verbose logging

For full CLI options:

```bash
uv run schema-bridge export --help
```

---

## Output formats

Use `--format` to select a single output format:

```
csv | json | jsonld | ttl | rdfxml | nt
```

Export commands write to **stdout**. Redirect to a file to persist output.

---

## GraphQL endpoint resolution (export)

For export, the GraphQL endpoint is resolved in this order (highest priority first):

1. CLI `--graphql-endpoint`
2. Profile `fetch.endpoint`
3. Environment `SCHEMA_BRIDGE_GRAPHQL_ENDPOINT`
4. CLI `--base-url` + `--schema`
5. Profile `fetch.base_url` + `fetch.schema`
6. Environment `SCHEMA_BRIDGE_BASE_URL` + `SCHEMA_BRIDGE_SCHEMA`

No defaults are assumed. If nothing resolves, the CLI errors.

If a full endpoint is provided, `base_url/schema` are ignored.
Otherwise the endpoint is constructed as:

```
{base_url}/{schema}/graphql
```

---

## Export profiles

Export profiles are YAML files with the following sections:

* `fetch` — GraphQL query and data root
* `mapping` — flattening rows into the canonical graph
* `export` — SPARQL SELECT / CONSTRUCT
* `validate` — SHACL shapes and toggle

### Common export keys

* `fetch.graphql`

* `fetch.root_key`

* `fetch.endpoint` (optional)

* `fetch.base_url` + `fetch.schema` (optional)

* `export.select` / `export.construct`

* `validate.shacl`

* `validate.enabled`

### Available export profiles

* `dcat` (demo)
* `fdp` (demo)
* `health-dcat-ap-molgenis` (Health-DCAT-AP Release 5, partial coverage)

---

# Ingest pipeline

## Quick start (ingest)

Ingest profiles control how RDF is converted into rows and uploaded via GraphQL mutations.

```bash
uv run schema-bridge ingest path/to/input.ttl \
  --profile ingest-dcat \
  --base-url https://emx2.dev.molgenis.org/ \
  --schema catalogue-demo \
  --dry-run
```

```bash
uv run schema-bridge ingest path/to/input.ttl \
  --profile ingest-dcat \
  --out out/rows.json \
  --dry-run
```

Notes:

* `--format` is optional; RDF format is inferred from the file extension
* Use `--dry-run` or `--out` to inspect rows without uploading

---

## GraphQL target resolution (ingest)

GraphQL targets for ingest are resolved in this order:

1. CLI `--base-url` / `--schema`
2. Ingest profile `graphql.base_url` / `graphql.schema`
3. Environment `SCHEMA_BRIDGE_BASE_URL` / `SCHEMA_BRIDGE_SCHEMA`
4. Built-in defaults

---

## Ingest profiles

Ingest profiles are YAML files with these sections:

* `validate` — SHACL validation
* `extract` — SPARQL SELECT over RDF
* `upload` — target table and mutation behavior
* `graphql` — default GraphQL configuration (optional)

### Common ingest keys

* `validate.shacl`

* `validate.enabled`

* `extract.sparql`

* `upload.table`

* `upload.mode` (`insert` or `upsert`)

* `upload.id_prefix`

* `upload.batch_size`

* `graphql.base_url` + `graphql.schema`

* `graphql.token` (optional)
