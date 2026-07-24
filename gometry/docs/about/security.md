---
description: gometry's security posture for untrusted geometry input, parsing, pickle, and fail-fast validation.
---

# Security & untrusted input

gometry is a local geometry engine, not a sandbox. The security boundary is the
data format you choose at ingress and the validation you run before trusting
the parsed geometry.

## Accepted untrusted formats

Use these formats for untrusted data:

- WKB/EWKB via `gm.from_wkb`;
- WKT/EWKT via `gm.from_wkt`;
- GeoJSON via `gm.from_geojson` or `gm.from_features`;
- Arrow/GeoArrow/GeoParquet when the producer is a data system, not arbitrary
  Python code.

The readers fail fast with structured `ParseError`, `CRSError`,
`InvalidGeometryError`, or `GeometryError` lanes. Parse untrusted bytes with these
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

Validate topology with `geom.validate()` / `arr.is_valid` and repair deliberately
with `repair()` when your workflow accepts repaired shapes.

## Ingress threat model

gometry is an ordinary native extension (like pyarrow, numpy, or polars), not a
sandbox. The design target is **no amplification**: a small or O(1)-sized input
must not force disproportionate memory or CPU via forged lengths, lying
iterators, or unbounded type recursion. Structural validation (offset
monotonicity, BinaryView prefixes, validity bitmaps, exact encoding shapes)
rejects **hidden/forged** sizes; it does **not** wall off merely-large-and-valid
payloads.

What gometry **does** guarantee:

- Memory and work stay **bounded relative to the input** you actually supplied
  (no attacker leverage from tiny forged declarations).
- Malformed layout/data raises typed domain errors — never a panic across the
  FFI for ABI-conforming input that is not proportionally huge.
- Coverage factories expose one overridable size knob, `max_cells` (default
  `1_000_000` on H3, S2, geohash, and tile cover; pass `None` for unlimited).
  S2's adaptive coverer (when `level` is omitted) additionally takes
  `target_cells` (default `8`) as its approximation target — that is a quality
  budget, not a hard cap. Parsers impose no flat byte or feature-count ceiling,
  but WKT, WKB, and GeoJSON ingestion reject excessive recursive nesting.
  Generated-work and result-count limits also protect expansion, grid collection
  and uncompact, transform-bounds densification, and CRS search. Bare
  unsized/infinite input iterables remain deliberately supported without an
  element ceiling (a lying `__len__` only tempers the reservation; it never caps
  or rejects).

What is **out of the threat model** (document, do not chase):

- **Abort on genuine OOM** when the input is proportionally large (bytes ≈
  memory) and the process rlimit is below what that input needs. Callers
  parsing untrusted data **must bound input size at the trust boundary**
  (`len(payload)`, feature count, decompressed size). Making every `Vec` /
  string allocation fallible under arbitrary rlimits is neither achievable nor
  required. This includes a bare unsized stream with no `__len__` consumed to
  memory exhaustion: it is proportional input, not amplification. In
  particular, each `Polygon(..., holes=stream)` hole owns `CoordSeq` columns
  backed by `Arc<[f64]>`; std has no fallible Arc-slice constructor, so under a
  hard `RLIMIT_AS` its inner allocation can abort before the outer fallible
  collector reports `MemoryError`.
- **Sized inputs and lying-`__len__` inputs** retain the catchable
  `MemoryError` guarantee for capacity growth: untrusted length hints are
  clamped before fallible reservation, so they cannot force an oversized
  up-front allocation.
- **Forged/lying duck-typed Arrow-C producers** that violate the C Data
  Interface ABI (no buffer capacity in the format). See
  [Arrow C capsule trust model](../ecosystem/arrow.md#arrow-c-capsule-trust-model).

## Resource limits belong at the caller boundary

Syntactically valid geometry can still exhaust CPU or memory through enormous
payloads, vertex counts, feature collections, or expensive topology. WKT, WKB,
and GeoJSON nesting is rejected at a fixed defensive ceiling; generated-work and
result-count limits also bound selected expansion, grid, transform, and catalog
operations. Those safeguards do not replace an application's byte, vertex,
feature-count, memory, or execution-time budget.

Reject oversized payloads before parsing, cap decompressed input separately,
bound batch and feature counts, and run hostile or multi-tenant workloads in a
process with OS memory/CPU limits and a timeout. Treat `repair`, overlay,
validation, and exact spatial refinement as potentially expensive work after a
successful parse. Never rely on a Python thread timeout as hard isolation.

## No-panic invariant

Malformed content, huge-but-finite coordinates, missing rows, CRS mismatch, and
invalid dimensionality must raise Python exceptions or return documented null
lanes; they must not panic across the FFI boundary.

## Testing approach

gometry does not ship or run a fuzzing campaign. The suite is a hand-written
deterministic corpus: malformed-input handling is covered by focused regression
tests that pin each specific defect, rather than by randomized mutation. This is
a deliberate choice — generated campaigns are slow, and every defect one finds
has to be pinned as a deterministic test anyway.

## Never unpickle untrusted data

Pickle is for trusted Python object persistence only. It can execute Python code
while loading. For untrusted or cross-service data, use WKB, WKT, GeoJSON,
GeoArrow, or GeoParquet.

## Reporting vulnerabilities

Do not post suspected vulnerabilities with exploit details in a public issue.
Use GitHub private vulnerability reporting on the monorepo that hosts gometry:

- [github.com/Zaczero/pkgs/security](https://github.com/Zaczero/pkgs/security)

If private reporting is unavailable, contact the maintainers listed in package
metadata rather than filing a public issue with exploit detail. Security fixes
target the supported release line documented in
[Compatibility](compatibility.md).

## See also

- [Errors & exceptions](../guide/errors.md) — `ParseError`, `InvalidGeometryError`, structured attrs.
- [Validation & repair](../guide/validation.md) — `validate` / `repair` at the boundary.
