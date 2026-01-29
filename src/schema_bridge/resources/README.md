# Resources

This folder contains all packaged inputs for the CLI.

```
resources/
  profiles/    # profile folders (profile.yml + graphql/sparql)
  sparql/      # shared SPARQL queries (mostly ingest)
  shacl/       # SHACL shapes
  rml/         # RML mappings (optional)
```

Conventions:

- Use paired `*_select.sparql` and `*_construct.sparql` for the same use case.
- Profiles should reference files by relative path inside their folder (e.g. `graphql/query.graphql`).
- Keep mapping YAML minimal: only add `field_paths` when needed for nested flattening.
- RML examples live under `rml/` and are derived from the RML spec.
