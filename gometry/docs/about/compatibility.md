---
description: gometry's v1 compatibility stance, semantic versioning, deprecation window, and no-alias policy.
---

# Compatibility & deprecation policy

gometry 1.0.0 is the first public release. From this version:

- gometry follows semantic versioning for documented public Python APIs;
- patch releases fix bugs and documentation without intentionally breaking the
  public contract;
- minor releases may add APIs and may deprecate APIs with warnings before
  removal;
- incompatible public removals wait for the next major release unless the API is
  security-sensitive or demonstrably unusable as documented.

## Deprecation window

The normal deprecation window is at least one minor release with a runtime
warning and a changelog entry before removal. The warning names the replacement
when one exists.

## Numeric parsing guarantees

WKT ingest parses ordinates bit-exactly (shortest-round-trip output re-imports
to the identical `float64`). GeoJSON ingest uses serde_json's fast float path:
decimal literals with more than 17 significant digits may round 1 ULP
differently than a bit-exact parser (about 1 part in 10^16 — far below any
physical coordinate precision). This is a deliberate trade: the bit-exact mode
measured 10-13% slower on FeatureCollection ingest. Coordinates written by
gometry always round-trip exactly in both formats.

## No legacy aliases

gometry does not keep legacy aliases just to preserve a second spelling. A second
spelling exists only when it adds a distinct capability. That is why unary
geometry facts are properties (`geom.area`), unary transforms are methods
(`geom.buffer(...)`), and binary relationships are free functions
(`gm.contains(a, b)`).

## See also

- [Design principles](design.md) — the API constitution behind the no-alias policy.
- [Migrating to gometry](../migrating/index.md) — old-stack → canonical spelling.
- [Migration cheatsheet](../migrating/cheatsheet.md) — searchable symbol tables.
