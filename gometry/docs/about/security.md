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
- GeoJSON via [`gm.from_geojson`][gometry.from_geojson], including feature metadata with
  `features=True`;
- Arrow/GeoArrow/GeoParquet after accepting the provider as a trusted ABI
  participant; the buffers and geometry values are still treated as untrusted
  contents.

The readers fail fast with structured [`ParseError`][gometry.ParseError], [`CRSError`][gometry.CRSError],
[`InvalidGeometryError`][gometry.InvalidGeometryError], or [`GeometryError`][gometry.GeometryError]. Parse untrusted bytes with these
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
sandbox. Its ingress boundaries validate the structures they consume: for
example, Arrow offsets and validity, buffer spans, and supported encoding
shapes are checked before decoding. These checks reject malformed structure;
they do not impose a general payload-size or CPU limit. Arrow providers must
also obey the exported structures, readable spans, capsule names, and release
callbacks; see [Arrow C capsule trust model](../ecosystem/arrow.md#arrow-c-capsule-trust-model).

## Collection and allocation

Python's `__len__` is treated as the provider's exact size contract and is used
to reserve collection capacity. A provider that reports an inaccurate length
can request a disproportionate initial reservation; gometry does not recount
the iterable before reserving. Inputs without `__len__` grow as values are
consumed. There is no universal element ceiling for finite streams, and an
unbounded stream can keep consuming memory and CPU.

Exceptions raised by a Python provider propagate, including a `MemoryError`
raised by that provider. Native allocator exhaustion is not converted into a
catchable `MemoryError` guarantee and can abort the process. Applications
accepting untrusted payloads should enforce their own input-size and batch
limits before handing data to the extension.

## Generated-output and work limits

Coverage factories and H3 traversal methods default to `max_cells=1_000_000`;
passing `max_cells=None` removes that output-count limit. This limit does not
set a CPU deadline for classification or traversal. WKT, WKB, and GeoJSON
parsers reject excessive recursive nesting, but impose no general byte or
feature-count ceiling. [`CellArray.uncompact`][gometry.CellArray.uncompact]
has no result-count limit.

Other limits belong to specific operations. When an operation documents a
generated-output cap, [`ResourceLimitError.code`][gometry.ResourceLimitError.code] is
`'output_limit'` for that refusal. A bounded exact search may return
`code == 'work_limit'` when it stops before certification; that does not prove
no acceptable result exists.
An `OverflowError` with `code == 'output_unrepresentable'` means the operation could not satisfy its
admitted output and preservation contract in its coordinate domain. None of
these operation-specific errors is a general CPU deadline or a global resource
guarantee.

Syntactically valid geometry can still exhaust CPU or memory through enormous
payloads, vertex counts, feature collections, or expensive topology. Run hostile
or multi-tenant workloads in a process with OS memory/CPU limits and a timeout.
Treat [`repair`][gometry.Geometry.repair], overlay, validation, and exact spatial refinement as potentially
expensive work after a successful parse.
Python thread timeouts are not hard isolation.

<a id="no-panic-invariant"></a>

## Never unpickle untrusted data

[`pickle`](https://docs.python.org/3/library/pickle.html) is for trusted Python object persistence only. It can execute Python code
while loading. For untrusted or cross-service data, use WKB, WKT, GeoJSON,
GeoArrow, or GeoParquet.

## Reporting vulnerabilities

A suspected vulnerability goes to GitHub's
[private vulnerability reporting](https://github.com/Zaczero/pkgs/security/advisories/new)
instead of the public issue tracker.

If private reporting is unavailable, contact the maintainers listed in package
metadata. Security fixes ship in a normal release;
[Compatibility](compatibility.md) states the supported runtimes.

## See also

- [Errors & exceptions](../guide/errors.md) — [`ParseError`][gometry.ParseError], [`InvalidGeometryError`][gometry.InvalidGeometryError], structured attrs.
- [Validation & repair](../guide/validation.md) — [`validate`][gometry.Geometry.validate] / [`repair`][gometry.Geometry.repair] at the boundary.
