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

WKT and GeoJSON ingest both parse ordinates bit-exactly: a finite decimal that
gometry writes (shortest round-trip) re-imports to the identical `float64`, and
hostile long decimals match correctly-rounded binary64 (no silent 1 ULP drift).
GeoJSON uses serde_json's `float_roundtrip` parser on read and `zmij` on write.

**Geometry coordinates** are binary64. Decimal text is correctly rounded, and
mapping / `__geo_interface__` integers are admitted only when exactly
representable as binary64 — every integer with magnitude ≤ 2⁵³, plus larger
integers whose lower bits are zero under the 53-bit significand (for example
2⁵³+2 and 2⁶⁰). A non-exact mapping integer raises `ParseError`; an integer
token outside the signed/unsigned 64-bit text parser range follows ordinary
finite binary64 JSON-number parsing.

**Feature properties and ids are opaque Python side data, not geometry
coordinates.** Text and one-byte-buffer `from_features` preserve every JSON
integer lexeme as an arbitrary-precision Python `int`, at every nesting depth;
mapping input retains its original Python integer. Their finite decimal leaves
use the same correctly-rounded parser as coordinates. No Feature side-data
number is staged through binary64 merely because it arrived as GeoJSON text.

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
