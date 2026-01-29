# Schema Bridge

**TL;DR**: Schema Bridge provides a reversible pipeline between GraphQL-shaped data and RDF-based metadata standards, using a canonical RDF graph as an intermediate layer to decouple APIs from export and ingest logic.

## Table of Contents

* [Summary](#summary)
* [Example: MOLGENIS Catalogue → Health-DCAT-AP](#example-molgenis-catalogue--health-dcat-ap)
* [Conceptual overview](#conceptual-overview)
* [Installation](#installation)
* [Profiles](#profiles)

  * [Profile kinds](#profile-kinds)
  * [Export profiles](#export-profiles)
  * [Ingest profiles](#ingest-profiles)
* [GraphQL endpoint resolution](#graphql-endpoint-resolution)
* [Why a canonical RDF layer?](#why-a-canonical-rdf-layer)
* [Export pipeline](#export-pipeline)

  * [Quick start (export)](#quick-start-export)
  * [Output formats](#output-formats)
* [Ingest pipeline](#ingest-pipeline)

  * [Quick start (ingest)](#quick-start-ingest)

---

## Summary

Schema Bridge is a small, standalone pipeline for transforming GraphQL-shaped data into RDF (e.g. Health-DCAT-AP, FDP) or tabular formats, and for ingesting RDF metadata back into a GraphQL backend.

More abstractly, Schema Bridge attempts to solve the problem of **bidirectional interchange between concrete API-shaped data and standardised metadata representations**, without hard-coding schemas or mappings into application logic. In other words: it separates how data is exposed by an API from how that same information is represented for interchange, allowing the two to evolve independently while remaining convertible in both directions.

It is profile-driven (YAML): the same pipeline can target different GraphQL schemas, mapping conventions, and output standards without code changes.

Core implementation locations:

* CLI entrypoint and commands: [`src/schema_bridge/cli.py`](src/schema_bridge/cli.py)
* Profile definitions and assets (YAML, GraphQL, SPARQL, SHACL): [`src/schema_bridge/resources/profiles/`](src/schema_bridge/resources/profiles/)
* Tests (unit + integration): [`tests/`](tests/)
* Pipeline implementation (fetch, mapping, export, ingest helpers): [`src/schema_bridge/pipeline/`](src/schema_bridge/pipeline/)

---

## Example: MOLGENIS Catalogue → Health-DCAT-AP

A concrete use case is exporting the **MOLGENIS Catalogue** to **Health-DCAT-AP Release 5** ([ref](https://healthdataeu.pages.code.europa.eu/healthdcat-ap/releases/release-5/)), as used by [molgeniscatalogue.org](https://molgeniscatalogue.org).

Schema Bridge provides a packaged profile, `healthdcat-ap-r5-molgenis`, which maps catalogue data exposed via GraphQL into a Health-DCAT-AP Release 5–compatible RDF representation.

This profile emits:

* A `dcat:Catalog` containing datasets and catalog records
* A `dcat:CatalogRecord` for each dataset
* A `dcat:Dataset` with landing page, publisher, contact point, spatial and temporal coverage, and themes/keywords
* One or more `dcat:Distribution` resources per dataset, including access URLs and access-rights or policy links when available
* **Notes on scope:** The mapping reflects a pragmatic interpretation of Health-DCAT-AP applied to the fields currently exposed by the catalogue schema; coverage of optional constructs is therefore partial.

Example usage:

```bash
uv run schema-bridge export \
  --profile healthdcat-ap-r5-molgenis \
  --format rdfxml
```

```bash
uv run schema-bridge export \
  --profile healthdcat-ap-r5-molgenis \
  --format jsonld \
  --limit 10
```

Sample output (live run, public endpoint, January 29, 2026):

```ttl
<https://catalogue.org/resource/AHON> a dcat:Dataset ;
    dcterms:identifier "AHON" ;
    dcterms:title "Academisch Huisarts Ontwikkel Netwerk" ;
    dcat:landingPage <https://www.umcg.nl/-/ahon> ;
    dcat:distribution <https://molgeniscatalogue.org/catalogue/distribution/AHON> ;
    odrl:hasPolicy <http://purl.obolibrary.org/obo/DUO_0000006>,
        <http://purl.obolibrary.org/obo/DUO_0000042> .

<https://molgeniscatalogue.org/catalogue/distribution/AHON> a dcat:Distribution ;
    dcterms:description "Every three months new data are added to the longitudinal AHON-database." ;
    dcterms:rights "Access fee: true" ;
    dcat:accessURL <https://www.umcg.nl/-/ahon> ;
    odrl:hasPolicy <http://purl.obolibrary.org/obo/DUO_0000006>,
        <http://purl.obolibrary.org/obo/DUO_0000042> .
```

This section is illustrative: the same pipeline can target other domains, schemas, or metadata standards by swapping profiles.

---

## Conceptual overview

Schema Bridge implements two symmetric pipelines:

1. **Export**: GraphQL → canonical RDF → serialized outputs
2. **Ingest**: RDF → extracted rows → GraphQL mutations

Both pipelines are configured entirely through profiles.

---

## Installation

```bash
uv sync --extra test
```

---

## Profiles

Profiles are the primary configuration unit. They live under:

```
src/schema_bridge/resources/profiles/<profile>/profile.yml
```

A profile can be referenced by name, profile directory, or direct path to `profile.yml`.

### Profile kinds

Each profile declares a `kind`:

* `export` → drives the export pipeline
* `ingest` → drives the ingest pipeline

### Export profiles

Export profiles are YAML files with these sections:

* `fetch` — GraphQL query and data root
* `mapping` — rules for projecting API-shaped data into the canonical RDF graph
* `export` — SPARQL SELECT / CONSTRUCT
* `validate` — SHACL shapes and toggle

Common export keys:

* `fetch.graphql`
* `fetch.root_key`
* `fetch.endpoint` (optional)
* `fetch.base_url` + `fetch.schema` (optional)
* `export.select` / `export.construct`
* `validate.shacl`
* `validate.enabled`

Available export profiles in this repo:

* `dcat` (demo)
* `fdp` (demo)
* `healthdcat-ap-r5-molgenis` (Health-DCAT-AP Release 5, partial coverage)

#### Mapping example (why it exists)

Mappings define how **GraphQL-shaped rows** are projected into a **canonical RDF graph** that remains stable across profiles and exports.

GraphQL APIs expose tree-shaped results: nested objects, optional branches, repeated structures, and multiple fields that encode the same concept. Their meaning is implicit in structure. SPARQL, by contrast, operates strictly over **triples** (subject–predicate–object relations), with no notion of nesting or application-level shape. The mapping layer translates between these representations.

Rather than running SPARQL directly over API-shaped data, Schema Bridge first normalizes the input into a canonical graph. SPARQL then operates over that intermediate representation.

Minimal example:

```yaml
mapping:
  field_paths:
    countryNames: countries[].name
    contactEmail:
      - contactPoint.email
      - contactEmail
```

What this expresses:

* `countries[].name` flattens nested lists into a repeated field (`countryNames`).
* `contactEmail` defines a fallback across alternative source fields.

Across a full profile, mappings typically:

* flatten nested collections into repeatable fields,
* unify multiple source paths under one canonical field name,
* mark selected fields as IRIs rather than literals.

This is what allows SPARQL constructs to be written once against a stable intermediate graph, while the source API schema remains free to evolve.

### Ingest profiles

Ingest profiles are YAML files with these sections:

* `validate` — SHACL validation
* `extract` — SPARQL SELECT over RDF
* `upload` — target table and mutation behavior
* `graphql` — default GraphQL configuration (optional)

Common ingest keys:

* `validate.shacl`
* `validate.enabled`
* `extract.sparql`
* `upload.table`
* `upload.mode` (`insert` or `upsert`)
* `upload.id_prefix`
* `upload.batch_size`
* `graphql.base_url` + `graphql.schema`
* `graphql.token` (optional)

---

## GraphQL endpoint resolution

Schema Bridge resolves the GraphQL endpoint (base URL + schema, or a full endpoint URL) the same way across export and ingest: CLI overrides profile, which overrides environment. If no location can be resolved, the CLI errors.

Resolution order (highest priority first):

1. CLI full endpoint `--graphql-endpoint` *(if supported by the command)*
2. Profile full endpoint (e.g. `fetch.endpoint` or `graphql.endpoint`)
3. Environment `SCHEMA_BRIDGE_GRAPHQL_ENDPOINT`
4. CLI `--base-url` + `--schema`
5. Profile base URL + schema (e.g. `fetch.base_url` + `fetch.schema`, or `graphql.base_url` + `graphql.schema`)
6. Environment `SCHEMA_BRIDGE_BASE_URL` + `SCHEMA_BRIDGE_SCHEMA`

If a full endpoint is provided, `base_url/schema` are ignored. Otherwise the endpoint is constructed as:

```text
{base_url}/{schema}/graphql
```

---

## Why a canonical RDF layer?

### TL;DR

Schema Bridge uses RDF as a canonical intermediate representation because it provides a stable, schema-flexible graph model for heterogeneous data. GraphQL results are lifted into a canonical graph, transformed and optionally validated declaratively, then serialized or re-materialized. RDF is used strictly as an interchange layer; no persistent triple store, reasoning engine, or ontology commitment is assumed.

### Explanation

At its core, RDF provides a minimal way of expressing relationships between entities. Information is represented as triples consisting of a subject, a predicate, and an object. Each element is identified by a URI, which allows relationships to be named explicitly rather than implied by structure. Taken together, these triples form a directed, labeled graph.

This representation has practical consequences: schemas can evolve without invalidating existing data, and data from independent systems can be merged without prior coordination because shared identifiers act as join points. Partial data is representable without placeholders, and absence of information does not require schema changes.

GraphQL APIs tend to expose application-specific structures, while metadata standards impose different conceptual models. A canonical graph representation provides a neutral middle layer in which these differences can be reconciled explicitly.

Schema Bridge treats RDF as a temporary, inspectable representation that sits between concrete systems:

```text
GraphQL rows  →  canonical RDF graph  →  standard exports
standard RDF →  extracted rows       →  GraphQL mutations
```

This keeps mappings declarative, validation orthogonal, and export and ingest conceptually symmetric.

---

# Export pipeline

## Quick start (export)

Export profiles wire together **fetch → mapping → export → validation**.

```bash
uv run schema-bridge export --profile dcat --format ttl
uv run schema-bridge export --profile fdp --format ttl
uv run schema-bridge export --profile healthdcat-ap-r5-molgenis --format ttl
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

`csv | json | jsonld | ttl | rdfxml | nt`

Export commands write to `stdout`. Redirect to a file to persist output.

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
