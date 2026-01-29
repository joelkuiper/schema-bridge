# Schema Bridge

**TL;DR**: Schema Bridge provides a reversible pipeline between GraphQL-shaped data and RDF-based metadata standards, using a canonical RDF graph as an intermediate layer to decouple APIs from export and ingest logic.

## Table of Contents

* [Summary](#summary)
* [Example: MOLGENIS Catalogue → Health-DCAT-AP](#example-molgenis-catalogue--health-dcat-ap)
* [Architecture](#architecture)
  * [Pipeline overview](#pipeline-overview)
  * [Why a canonical RDF layer?](#why-a-canonical-rdf-layer)
  * [How mappings bridge GraphQL and RDF](#how-mappings-bridge-graphql-and-rdf)
* [Installation](#installation)
* [Quick start](#quick-start)
* [Profiles](#profiles)
  * [Export profiles](#export-profiles)
  * [Ingest profiles](#ingest-profiles)
* [GraphQL endpoint resolution](#graphql-endpoint-resolution)
* [Output formats](#output-formats)

---

## Summary

Schema Bridge is a small, standalone pipeline for transforming GraphQL-shaped data into RDF (e.g. Health-DCAT-AP, FDP) or tabular formats, and for ingesting RDF metadata back into a GraphQL backend.

More abstractly, Schema Bridge attempts to solve the problem of "bidirectional interchange between concrete API-shaped data and standardised metadata representations", without hard-coding schemas or mappings into application logic. In other words: it separates how data is exposed by an API from how that same information is represented for interchange, allowing the two to evolve independently while remaining convertible in both directions.

It is profile-driven (YAML): the same pipeline can target different GraphQL schemas, mapping conventions, and output standards without code changes.

Core implementation locations:

* CLI entrypoint and commands: [`src/schema_bridge/cli.py`](src/schema_bridge/cli.py)
* Profile definitions and assets (YAML, GraphQL, SPARQL, SHACL): [`src/schema_bridge/resources/profiles/`](src/schema_bridge/resources/profiles/)
* Tests (unit + integration): [`tests/`](tests/)
* Pipeline implementation (fetch, mapping, export, ingest helpers): [`src/schema_bridge/pipeline/`](src/schema_bridge/pipeline/)
* RDF backend helper ([Oxigraph](https://github.com/oxigraph/oxigraph) store via [oxrdflib](https://github.com/oxigraph/oxrdflib): [`src/schema_bridge/rdf.py`](src/schema_bridge/rdf.py)

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

## Architecture

## Pipeline overview

Schema Bridge implements two symmetric pipelines, both configured entirely through profiles:

```text
Export:  GraphQL → canonical RDF graph → serialized outputs
Ingest:  RDF     → extracted rows      → GraphQL mutations
```

Although these pipelines run in opposite directions, they follow the same underlying idea: GraphQL APIs and RDF-based metadata standards describe the same information, but in very different shapes.

Rather than translating directly between those shapes, Schema Bridge introduces a canonical RDF graph as a neutral intermediate layer. All transformation, validation, export, and ingest steps operate against this shared representation. This keeps mappings declarative, validation optional and orthogonal, and export and ingest conceptually symmetric.

---

## Why a canonical RDF layer?

Schema Bridge uses RDF as an intermediate representation because it provides a stable, schema-flexible graph model well suited to heterogeneous metadata.

GraphQL results are first lifted into a canonical RDF graph. That graph can then be transformed, queried, or validated declaratively, before being serialized to standard formats or re-materialized back into GraphQL mutations. RDF is used strictly as an interchange layer; no persistent triple store, reasoning engine, or ontology commitment is assumed.

At a basic level, RDF expresses information as explicit relationships. Each statement is a triple—subject, predicate, object—identified by URIs. Taken together, these triples form a directed, labeled graph in which meaning is expressed by named relations rather than by structural position.

This has several practical consequences that are especially relevant for metadata transformation:

* Schemas can evolve safely: new predicates can be added without invalidating existing data.
* Independent data sources can be merged without prior coordination, as shared identifiers act as natural join points.
* Partial data is first-class: missing information does not require placeholders or schema changes.
* Structure does not imply meaning: relationships are explicit, not inferred from nesting or field layout.

These properties closely match the realities of working with metadata. GraphQL APIs typically expose application-specific, tree-shaped views of data, while metadata standards impose their own conceptual models. A canonical graph provides a neutral middle layer where those differences can be reconciled explicitly, rather than implicitly encoded in ad-hoc transformations.

---

## How mappings bridge GraphQL and RDF

Mappings define how GraphQL-shaped rows are projected into the canonical RDF graph.

GraphQL APIs expose nested, tree-shaped results: optional branches, repeated structures, and multiple fields that may encode the same concept in slightly different ways. Much of their meaning is implicit in structure. SPARQL, by contrast, operates purely over triples—subject–predicate–object relations—with no notion of nesting or application-level shape.

The mapping layer is where this translation happens.

Instead of running SPARQL directly over API-shaped data, Schema Bridge first normalizes incoming data into the canonical graph. SPARQL queries are then written once against that stable representation. This decouples transformations from the source API schema, allowing APIs to evolve without requiring changes to downstream queries or exports.

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

* `countries[].name` flattens a nested list into a repeatable canonical field (`countryNames`).
* `contactEmail` defines a fallback across multiple possible source paths.

Across a full profile, mappings typically flatten nested collections into repeatable fields, unify multiple source paths under a single canonical field, and mark selected fields as IRIs rather than literals.



---

## Installation

```bash
uv sync --extra test
```

The runtime uses the Oxigraph-backed RDF store via `oxrdflib` (already included in the dependency set).

---

## Quick start

### Export

Export profiles wire together **fetch → mapping → export → validation**.

```bash
uv run schema-bridge export --profile dcat --format ttl
uv run schema-bridge export --profile fdp --format ttl
uv run schema-bridge export --profile healthdcat-ap-r5-molgenis --format ttl
```

Useful options:

* `--format` — output format (see [Output formats](#output-formats))
* `--limit` — limit GraphQL rows fetched
* `--debug` — verbose logging

For full CLI options: `uv run schema-bridge export --help`

### Ingest

Ingest profiles wire together **parse → select → row shaping → mutation**.

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

## Profiles

Profiles are the primary configuration unit. They live under:

```
src/schema_bridge/resources/profiles/<profile>/profile.yml
```

A profile can be referenced by name, profile directory, or direct path to `profile.yml`. Each profile declares a `kind` (`export` or `ingest`) that determines which pipeline it drives.

### Export profiles

Export profiles are YAML files with four sections:

| Section | Purpose |
|---------|---------|
| `fetch` | GraphQL query and data root |
| `mapping` | Rules for projecting API-shaped data into the canonical RDF graph |
| `export` | SPARQL SELECT / CONSTRUCT |
| `validate` | SHACL shapes and toggle |

**Configuration keys:**

```yaml
fetch:
  graphql: <query file>
  root_key: <path to data in response>
  endpoint: <full URL>              # optional, overrides base_url + schema
  base_url: <base URL>              # optional
  schema: <schema name>             # optional

mapping:
  field_paths: <field mappings>     # see Architecture > How mappings work

export:
  select: <path to SPARQL SELECT>
  construct: <path to SPARQL CONSTRUCT>

validate:
  shacl: <path to shapes file>
  enabled: true|false
```

**Available export profiles:**

| Profile | Description |
|---------|-------------|
| `dcat` | Demo profile |
| `fdp` | Demo profile |
| `healthdcat-ap-r5-molgenis` | Health-DCAT-AP Release 5 (partial coverage) |

### Ingest profiles

Ingest profiles are YAML files with four sections:

| Section | Purpose |
|---------|---------|
| `validate` | SHACL validation |
| `extract` | SPARQL SELECT over RDF |
| `upload` | Target table and mutation behavior |
| `graphql` | Default GraphQL configuration (optional) |

**Configuration keys:**

```yaml
validate:
  shacl: <shapes file>
  enabled: true|false

extract:
  sparql: <path to SPARQL SELECT>

upload:
  table: <target table>
  mode: insert|upsert
  id_prefix: <prefix for generated IDs>
  batch_size: <number>

graphql:
  base_url: <base URL>
  schema: <schema name>
  token: <auth token>
```

---

## GraphQL endpoint resolution

Schema Bridge resolves the GraphQL endpoint the same way across export and ingest: CLI overrides profile, which overrides environment. If no location can be resolved, the CLI errors.

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

## Output formats

Use `--format` to select an output format for export:

| Format | Extension |
|--------|-----------|
| `csv` | Tabular CSV |
| `json` | JSON |
| `jsonld` | JSON-LD |
| `ttl` | Turtle |
| `rdfxml` | RDF/XML |
| `nt` | N-Triples |

Export commands write to `stdout`. Redirect to a file to persist output.
