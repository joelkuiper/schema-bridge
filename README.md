# Schema Bridge

**TL;DR**: Schema Bridge provides a reversible pipeline between GraphQL-shaped data and RDF-based metadata standards, using a canonical RDF graph as an intermediate layer to decouple APIs from export and ingest logic.

## Table of Contents

* [Summary](#summary)
* [Installation](#installation)
* [Example: MOLGENIS Catalogue → Health-DCAT-AP + Schema.org](#example-molgenis-catalogue--health-dcat-ap--schemaorg)
* [Architecture](#architecture)
  * [Pipeline overview](#pipeline-overview)
  * [Why a canonical RDF layer?](#why-a-canonical-rdf-layer)
  * [How mappings bridge GraphQL and RDF](#how-mappings-bridge-graphql-and-rdf)
  * [Canonical RDF graph shape and querying](#canonical-rdf-mapping)
* [Quick start](#quick-start)
* [Profiles](#profiles)
  * [Export profiles](#export-profiles)
  * [Ingest profiles](#ingest-profiles)
* [GraphQL endpoint resolution](#graphql-endpoint-resolution)

---

## Summary

Schema Bridge is a small, standalone pipeline for transforming GraphQL-shaped data into RDF (e.g. Health-DCAT-AP, Schema.org) or tabular formats, and for ingesting RDF metadata back into a GraphQL backend.

More abstractly, Schema Bridge attempts to solve the problem of "bidirectional interchange between concrete API-shaped data and standardised metadata representations", without hard-coding schemas or mappings into application logic. In other words: it separates how data is exposed by an API from how that same information is represented for interchange, allowing the two to evolve independently while remaining convertible in both directions.

It exists to isolate metadata transformation from application logic, avoiding the need to embed RDF stacks, bespoke exporters, or schema-specific code into GraphQL services while still supporting reversible, standards-aligned interchange.
Schema Bridge treats RDF strictly as an interchange representation; it does not require a persistent semantic store, ontology-driven application logic, or server-side RDF infrastructure.

It is profile-driven (YAML): the same pipeline can target different GraphQL schemas, mapping conventions, and output standards without code changes.

Module map:

```
src/schema_bridge/
  cli.py                   CLI entrypoint and commands
  profiles/                profile loading + resolution
  graphql/                 GraphQL client + pagination
  rdf/                     mapping, SPARQL, SHACL, export helpers
  workflows/               export/ingest/materialize orchestration
  resources/               resources (including packaged profiles)
resources/profiles/        packaged profiles (YAML, GraphQL, SPARQL, SHACL)
tests/                     unit + integration tests
```

RDF backend: [Oxigraph](https://github.com/oxigraph/oxigraph) via [oxrdflib](https://github.com/oxigraph/oxrdflib).

---

## Installation

Install [uv](https://docs.astral.sh/uv/getting-started/installation/). Clone the repository, `cd` into it, then:

```bash
uv sync --extra test
```

Tests can be ran with `uv run pytest`.

---

## Example: MOLGENIS Catalogue → Health-DCAT-AP + Schema.org

A concrete use case is the export of the **MOLGENIS Catalogue** ([https://molgeniscatalogue.org](https://molgeniscatalogue.org)) to **Health-DCAT-AP Release 5** ([ref](https://healthdataeu.pages.code.europa.eu/healthdcat-ap/releases/release-5/)) to support FAIR and policy-aligned metadata publication, alongside a **Schema.org Dataset/DataCatalog** representation ([ref](https://schema.org/Dataset)) for web-scale discovery.

Schema Bridge provides two packaged profiles for this catalogue:

* `healthdcat-ap-r5-molgenis` — Health-DCAT-AP Release 5 export (partially complete)
* `schemaorg-molgenis` — Schema.org Dataset/DataCatalog export (partially complete)

Health-DCAT-AP focuses on rich catalog semantics (catalog records, distributions, policies, coverage), while Schema.org focuses on web-scale discovery semantics (Dataset/DataCatalog).

Health-DCAT-AP profile covers:

* `dcat:Catalog` + `dcat:CatalogRecord`
* `dcat:Dataset` with identifiers, titles, descriptions, landing pages, keywords/themes
* `dcat:Distribution` with access URLs, access-rights/policy links
* temporal coverage, spatial coverage, contacts, publisher, and HealthDCAT-AP extensions (health theme, population coverage, record counts)

Schema.org profile covers:

* `schema:DataCatalog` + `schema:Dataset`
* dataset name/description/identifier/url, keywords/about (themes)
* publisher + contact point
* `schema:DataDownload` with content URL and conditions of access

Example usage:

```bash
uv run schema-bridge export \
  --profile healthdcat-ap-r5-molgenis \
  --format rdfxml
```

```bash
uv run schema-bridge export \
  --profile healthdcat-ap-r5-molgenis \
  --format ttl \
  --limit 10
```

Schema.org example usage:

```bash
uv run schema-bridge export \
  --profile schemaorg-molgenis \
  --format jsonld \
  --limit 10
```

Health-DCAT-AP sample output (live run, public endpoint, January 29, 2026):

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

Schema.org sample output (live run, public endpoint, January 29, 2026):

```json
{
  "@context": {
    "schema": "https://schema.org/",
    "dcat": "http://www.w3.org/ns/dcat#",
    "dcterms": "http://purl.org/dc/terms/"
  },
  "@id": "https://catalogue.org/resource/BIG",
  "@type": "http://schema.org/Dataset",
  "http://schema.org/name": "Brain imaging Genomics",
  "http://schema.org/identifier": "BIG",
  "http://schema.org/about": {
    "@id": "http://edamontology.org/topic_3337"
  },
  "http://schema.org/contactPoint": [...]
}
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

GraphQL results are first lifted into a canonical RDF graph. That graph can then be transformed, queried, or validated declaratively, before being serialized to standard formats or re-materialized back into GraphQL mutations.

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

By default, nested objects (dicts/lists of dicts) are promoted into explicit RDF nodes. This creates a richer canonical graph without requiring manual `node_fields` definitions. You can disable this with `mapping.auto_nodes: false`, and you can control node IRIs with `mapping.node_defaults`.

Stable subject IRIs are controlled by `mapping.id_strategy`, which selects the identifier fields and template used to mint canonical subjects.

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


## Canonical RDF mapping

Schema Bridge maps GraphQL-shaped data into a flat, canonical RDF graph with predictable identifiers and predicates. This graph is designed to be easy to query with SPARQL and easy to transform into downstream standards such as DCAT or Schema.org.

### Namespaces and identifiers

By default, Schema Bridge uses a small set of fixed namespaces:

* Subjects live under a base URI (for example `https://catalogue.org/`)
* Canonical predicates live under `/field/`
* Entity types live under `/entity/`

### Subjects and types

Each GraphQL row becomes one RDF subject.

The subject IRI is generated from a stable identifier field (for example `id`), and the subject is given a type based on the profile configuration.

Example:

```
ex:resource/ABC123 a entity:Resource .
```

This means: “there is a Resource with identifier `ABC123`”.

### Fields become predicates

GraphQL fields are projected into canonical RDF predicates in a flat, uniform way.

* Each canonical field becomes a predicate under `field:`
* Values are literals by default
* Selected fields can be emitted as IRIs instead of literals

Example mapping:

```yaml
mapping:
  field_paths:
    landingPage: website
    contactEmail:
      - contactPoint.email
      - contactEmail
  iri_fields:
    - landingPage
```

Produces:

```
ex:resource/ABC123 field:landingPage <https://example.org/> .
ex:resource/ABC123 field:contactEmail "team@example.org" .
```

This keeps predicates stable and avoids encoding meaning in nested structure.

### Nested objects

Nested GraphQL objects are lifted into explicit RDF nodes.

By default, Schema Bridge does this automatically:

* Nested objects become linked nodes
* If a stable identifier is available, the node gets an IRI
* Otherwise, it becomes a blank node

Example:

```
ex:resource/ABC123 field:publisher _:pub .
_:pub field:name "Example Org" .
```

This preserves structure without forcing you to predefine schemas.

### Concepts and controlled vocabularies

Fields marked as concepts are represented as `skos:Concept` nodes and linked to the subject.

If the source provides a URI, it is preserved and linked using `owl:sameAs`.

This allows controlled vocabularies to be carried through the pipeline without special handling downstream.

### Querying and export

Because the canonical graph is flat and stable, SPARQL queries are simple and reusable.

Example:

```sparql
SELECT ?id ?title ?description WHERE {
  ?res field:id ?id ;
       field:title ?title .
  OPTIONAL { ?res field:description ?description }
}
```

Profiles then use SPARQL CONSTRUCT queries to transform this canonical graph into target standards such as DCAT or Schema.org, without referencing GraphQL-specific structure.

### End-to-end example

GraphQL input:

```json
{
  "id": "ABC123",
  "title": "Example Dataset",
  "description": "Short description."
}
```

Canonical RDF:

```ttl
ex:resource/ABC123 a entity:Resource ;
  field:id "ABC123" ;
  field:title "Example Dataset" ;
  field:description "Short description." .
```

Constructed DCAT:

```ttl
ex:resource/ABC123 a dcat:Dataset ;
  dct:identifier "ABC123" ;
  dct:title "Example Dataset" ;
  dct:description "Short description." .
```

That’s the idea: normalize once, then transform declaratively.

---

## Quick start

### Export

Export profiles wire together **fetch → mapping → export → validation**.

```bash
uv run schema-bridge export --profile dcat --format ttl
uv run schema-bridge export --profile schemaorg-molgenis --format jsonld
uv run schema-bridge export --profile healthdcat-ap-r5-molgenis --format ttl
```

Useful options:

* `--format` — output format (see table below)
* `--limit` — limit GraphQL rows fetched
* `--debug` — verbose logging

For full CLI options: `uv run schema-bridge export --help`

**Output formats:**

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

Profiles are the primary configuration unit. Packaged profiles live [here](./src/schema_bridge/resources/profiles).

You can also supply your own profiles. The `--profile` flag accepts:

* A packaged profile name (e.g. `dcat`)
* A profile folder (relative or absolute) containing `profile.yml`
* A direct path to `profile.yml` (relative or absolute)

Each profile declares a `kind` (`export` or `ingest`) that determines which pipeline it drives.

**Custom profile folder requirements:**

* `profile.yml` at the root
* Any referenced files alongside it (GraphQL query, SPARQL SELECT/CONSTRUCT, SHACL shapes, mapping YAML), or with resolvable paths

### Creating a new profile

A profile defines how Schema Bridge fetches data, projects it into the canonical RDF graph, and materializes the result.

At a minimum, creating a new profile involves:

* GraphQL operations that retrieve source data (queries, for export profiles) and optionally write extracted rows back to the target schema (mutations, for ingest profiles).
* One or more SPARQL queries:

  * `CONSTRUCT` a query for RDF-based exports (Turtle, JSON-LD, RDF/XML, N-Triples).
  * `SELECT` a query for tabular exports (JSON, CSV) or for ingest extraction.
* A `profile.yml` file that ties these assets together and configures optional mapping and validation steps.
* A stable ID strategy (`mapping.id_strategy`) and optional nested-node defaults (`mapping.node_defaults`) for the canonical RDF layer.

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
  id_strategy:
    template: <IRI template>
    fallback_fields: [<field>]
  node_defaults:
    subject_template: <IRI template>
    id_fields: [<field>]
  auto_nodes: true|false

export:
  select: <path to SPARQL SELECT>
  construct: <path to SPARQL CONSTRUCT>

validate:
  shacl: <path to shapes file>
  enabled: true|false

```

Notes:

* `id_strategy.template` controls how subject IRIs are minted. Use `{base_uri}`, `{path}`, and `{id}` placeholders.
* `id_strategy.fallback_fields` is the ordered list of fields used to pick a stable identifier.
* `node_defaults.subject_template` controls how nested-object node IRIs are minted.
* `node_defaults.id_fields` is the ordered list of nested-object fields used to pick node identifiers.
* `auto_nodes` toggles default promotion of nested objects into nodes.

**Packaged export profiles:**

| Profile | Description |
|---------|-------------|
| `dcat` | Demo profile |
| `schemaorg-molgenis` | Schema.org Dataset/DataCatalog (partial coverage) |
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

1. CLI full endpoint `--graphql-endpoint`
2. Profile full endpoint (e.g. `fetch.endpoint` or `graphql.endpoint`)
3. Environment `SCHEMA_BRIDGE_GRAPHQL_ENDPOINT`
4. CLI `--base-url` + `--schema`
5. Profile base URL + schema (e.g. `fetch.base_url` + `fetch.schema`, or `graphql.base_url` + `graphql.schema`)
6. Environment `SCHEMA_BRIDGE_BASE_URL` + `SCHEMA_BRIDGE_SCHEMA`

If a full endpoint is provided, `base_url/schema` are ignored. Otherwise the endpoint is constructed as:

```text
{base_url}/{schema}/graphql
```
