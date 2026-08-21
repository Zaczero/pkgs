---
description: gometry's security posture for untrusted geometry input, parsing, pickle, and fail-fast validation.
---

# Security & untrusted input

Security decisions depend on the data format chosen at ingress and the validation
run before trusting parsed geometry.

## Accepted untrusted formats

Use these formats for untrusted data contents:

- WKB/EWKB via [`gm.from_wkb`][gometry.from_wkb];
- WKT/EWKT via [`gm.from_wkt`][gometry.from_wkt];
- GeoJSON via [`gm.from_geojson`][gometry.from_geojson] or [`gm.from_features`][gometry.from_features];
- Arrow/GeoArrow/GeoParquet after accepting the provider as a trusted ABI
  participant; the buffers and geometry values are still treated as untrusted
  contents.

The readers fail fast with structured [`ParseError`][gometry.ParseError], [`CRSError`][gometry.CRSError],
[`InvalidGeometryError`][gometry.InvalidGeometryError], or [`GeometryError`][gometry.GeometryError] lanes. Parse untrusted bytes with these
readers, catch the structured exception, then validate or repair only the rows
your workflow accepts:

```python exec="on" source="block" result="text"
import gometry as gm

payloads = [
    b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@",
    b"not wkb",
]

for payload in payloads:
    try:
        geom = gm.from_wkb(payload, crs=4326)
    except gm.ParseError as err:
        print("rejected", err.format)
    else:
        print("accepted", geom.to_wkt())

```

Validate topology with [`geom.validate()`][gometry.Geometry.validate] / [`arr.is_valid`][gometry.GeometryArray.is_valid], and call [`repair()`][gometry.Geometry.repair]
where the workflow accepts repaired shapes.

## Ingress threat model

gometry is an ordinary native extension (like pyarrow, numpy, or polars), not a
sandbox. The design target is **no amplification**: a small or O(1)-sized input
must not force disproportionate memory or CPU via forged lengths, lying
iterators, or unbounded type recursion. Structural validation (offset
monotonicity, BinaryView prefixes, validity bitmaps, exact encoding shapes)
rejects **hidden/forged** sizes; it does **not** wall off merely-large-and-valid
payloads.

For an ABI-conforming Arrow provider, memory and work stay **bounded relative to
the input** actually supplied, without attacker leverage from tiny forged
declarations:

- Arrow providers must obey the exported structures, readable spans, capsule
  names, and release callbacks; see [Arrow C capsule trust model](../ecosystem/arrow.md#arrow-c-capsule-trust-model).
- Coverage factories expose one overridable size knob, `max_cells` (default
  `1_000_000` on H3, S2, geohash, and tile cover; pass `None` for unlimited).
  Parsers impose no flat byte or feature-count ceiling,
  but WKT, WKB, and GeoJSON ingestion reject excessive recursive nesting.
  Generated-work and result-count limits also protect expansion, grid collection,
  transform-bounds densification, and CRS search. [`CellArray.uncompact`][gometry.CellArray.uncompact] is a
  caller-directed, unlimited transform and is not protected by factory
  result-count limits.

Proportionally large input can still abort on genuine OOM when its bytes are
comparable to required memory and the process rlimit is below what it needs.
Callers parsing untrusted data **must bound input size at the trust boundary**
(`len(payload)`, feature count, decompressed size), reject oversized payloads
before parsing, cap decompressed input separately, and bound batch and feature
counts. Making every `Vec` / string allocation fallible under arbitrary rlimits
is neither achievable nor required. A bare unsized stream with no `__len__` can
also consume memory proportionally to its input. In particular, each
[`Polygon(..., holes=stream)`][gometry.Polygon] hole owns `CoordSeq` columns backed by `Arc<[f64]>`;
std has no fallible Arc-slice constructor, so under a hard `RLIMIT_AS` its inner
allocation can abort before the outer fallible collector reports `MemoryError`.

Bare unsized and infinite input iterables are supported without an element
ceiling. Sized inputs and lying-`__len__` inputs retain the catchable
`MemoryError` guarantee for capacity growth. The length hint is clamped before
fallible reservation, so it only tempers the reservation and never caps or
rejects input or forces an oversized up-front allocation.

Syntactically valid geometry can still exhaust CPU or memory through enormous
payloads, vertex counts, feature collections, or expensive topology. Run hostile
or multi-tenant workloads in a process with OS memory/CPU limits and a timeout.
Treat [`repair`][gometry.Geometry.repair], overlay, validation, and exact spatial refinement as potentially
expensive work after a successful parse.
Python thread timeouts are not hard isolation.

## No-panic invariant

Malformed content, huge-but-finite coordinates, missing rows, CRS mismatch, and
invalid dimensionality must raise Python exceptions or return documented null
lanes; they must not panic across the FFI boundary.

## Never unpickle untrusted data

Pickle is for trusted Python object persistence only. It can execute Python code
while loading. For untrusted or cross-service data, use WKB, WKT, GeoJSON,
GeoArrow, or GeoParquet.

## Reporting vulnerabilities

A suspected vulnerability goes to GitHub's
[private vulnerability reporting](https://github.com/Zaczero/pkgs/security/advisories/new)
on the monorepo that hosts gometry, not to the public issue tracker — a public
issue publishes the exploit detail alongside the report.

If private reporting is unavailable, contact the maintainers listed in package
metadata. Security fixes ship in a normal release;
[Compatibility](compatibility.md) states the supported runtimes.

## See also

- [Errors & exceptions](../guide/errors.md) — [`ParseError`][gometry.ParseError], [`InvalidGeometryError`][gometry.InvalidGeometryError], structured attrs.
- [Validation & repair](../guide/validation.md) — [`validate`][gometry.Geometry.validate] / [`repair`][gometry.Geometry.repair] at the boundary.
