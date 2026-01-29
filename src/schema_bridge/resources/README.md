# Resources

This folder contains all packaged inputs for the CLI.

```
resources/
  graphql/     # GraphQL queries
  sparql/      # SPARQL SELECT/CONSTRUCT queries
  profiles/    # YAML profiles
  shacl/       # SHACL shapes
  rml/         # RML mappings (optional)
```

Conventions:

- Use paired `*_select.sparql` and `*_construct.sparql` for the same use case.
- Profiles should reference files by relative path (e.g. `graphql/resources.graphql`).
- Keep mapping YAML minimal: only add `field_paths` when needed for nested flattening.
- RML examples live under `rml/` and are derived from the RML spec.
