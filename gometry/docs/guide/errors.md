---
description: Catch geometry errors, diagnose common failures, and identify failing array rows.
---

# Errors & exceptions

Catch errors by the action your application can take: fix an input type, correct
a value, reduce generated work, or handle a representation or resource failure.
Gometry's exception classes are available directly from `gometry`, such as
[`gm.CRSMismatchError`][gometry.CRSMismatchError].

```text
ValueError
└── GeometryError              invalid geometry, parameter value, or metadata
    ├── InvalidGeometryError        a geometry violates a structural or numeric rule
    ├── CRSError             a CRS could not be created, identified, exported, or used
    │   ├── CRSMismatchError operands carry incompatible CRS / coordinate-epoch metadata
    │   └── TransformError   a coordinate transform could not be built or failed to run
    └── ParseError           serialized input (WKT, WKB, GeoJSON, cell ids/tokens) is malformed

TypeError
└── GeometryTypeError        wrong geometry kind for the operation

RuntimeError
└── ResourceLimitError       output or computation budget exhausted
```

Grid-cell surfaces follow the same split as everything else: an out-of-range
`resolution`/`level`/`precision`/`zoom` is a plain `GeometryError` (a bad
parameter value), while an invalid cell id, token, or quadkey is a
[`ParseError`][gometry.ParseError] whose `.format` tag names the system (`'h3'`, `'s2'`,
`'geohash'`, `'tile'`, `'quadkey'`); codec tags are lowercase machine keys.

## Built-in exception compatibility

1. **Invalid values are `ValueError`s.** `GeometryError` and its subclasses
   identify bad data or parameter values; specific classes preserve recovery detail.
2. **Python protocol semantics stay builtin.** A non-number where a number is
   required is a plain `TypeError` (floats *and* integers); an out-of-range
   `arr[i]` is an `IndexError`; an exhausted iterator is `StopIteration`.
   Those protocol failures are not rewrapped.

[`GeometryTypeError`][gometry.GeometryTypeError] subclasses only `TypeError`:
handing [`line_substring`][gometry.Geometry.line_substring] a
[`Polygon`][gometry.Polygon] requires a different geometry kind, not a repaired
polygon. It is not caught by `except ValueError` or `except GeometryError`.

[`ResourceLimitError`][gometry.ResourceLimitError] subclasses `RuntimeError`.
It means a bounded computation stopped; it does not establish that the input is
invalid or that no result exists. `OverflowError` means a size or result cannot
be represented, including certified binary64 geometry output. Operating-system
failures remain `OSError`; Python allocation failures remain `MemoryError`.
Internal computation consistency failures use `RuntimeError`, not a bad-input
exception.

## Catching errors

The most specific exception class identifies the recovery strategy:

| I want to… | catch |
|---|---|
| reject a malformed scalar or batch ingest | [`ParseError`][gometry.ParseError] |
| reproject and retry on frame disagreements | [`CRSMismatchError`][gometry.CRSMismatchError] |
| fall back when a transform cannot be built or run | [`TransformError`][gometry.TransformError] |
| handle any CRS trouble in one place | [`CRSError`][gometry.CRSError] |
| reject invalid geometry, metadata, or parameter values | [`GeometryError`][gometry.GeometryError] |
| treat a wrong geometry kind as a type bug | `TypeError` *(or [`GeometryTypeError`][gometry.GeometryTypeError])* |
| treat any bad value like the stdlib does | `ValueError` |
| reduce generated output or work after a budget refusal | [`ResourceLimitError`][gometry.ResourceLimitError] |
| handle an unrepresentable result or size | `OverflowError` |

```python exec="on" source="block" result="text"
import gometry as gm

square = gm.box(0, 0, 2, 2)
tagged = gm.Point(1, 1, crs=4326)

try:
    gm.contains(square, tagged)
except gm.CRSMismatchError as e:
    print(type(e).__name__, '→', e)

```

A batch parser is fail-fast: one malformed row rejects the whole batch and the
exception identifies the row. If partial recovery is an explicit application
policy, parse rows individually and catch `ParseError`:

```python exec="on" source="block" result="text"
import gometry as gm

rows = ['POINT (1 2)', 'POINT (oops)', 'LINESTRING (0 0, 1 1)']
parsed = []
for row in rows:
    try:
        parsed.append(gm.from_wkt(row))
    except gm.ParseError:
        pass
print(len(parsed), 'of', len(rows), 'rows parsed')

```

```python exec="on" source="block" result="text"
import gometry as gm

polygon = gm.box(0, 0, 1, 1)
for catch in (TypeError, gm.GeometryTypeError):
    try:
        polygon.line_interpolate(0.5)
    except catch as e:
        print(f'caught as {catch.__name__}: {type(e).__name__}')

```

## Troubleshooting common errors

| You see | It means | Fix |
|---|---|---|
| `CRSMismatchError: ... CRS-free vs "EPSG:4326"` | one operand carries a CRS and the other doesn't (or they differ) | tag the bare one with `with_crs(...)`, or reproject with `to_crs(...)` — gometry never coerces frames |
| `CRSError: with_crs would re-tag CRS ... without moving coordinates` | you asked to *relabel* a geometry that already has a CRS | `to_crs(...)` to reproject; `with_crs(..., overwrite=True)` only if the original tag was wrong |
| `CRSError: ... epoch requires a CRS` | a coordinate epoch is meaningless without a frame | set the CRS first (or together: `Point(..., crs=..., epoch=...)`) |
| `InvalidGeometryError: ... cannot carry Z/M` (`to_geojson` on M or `to_polyline` on Z/M) | the target format has no slot for those ordinates | use WKB/WKT/GeoArrow, or explicitly clear dimensions with `with_coordinates(m=None)` / `force_2d()` when loss is intended |
| [`ParseError`][gometry.ParseError] with `.format` set | a serialized input (WKT/WKB/GeoJSON/GeoArrow/GeoParquet/pickle/cell id/quadkey) is malformed | check `e.format` for which codec rejected it; a batch fails as a unit and attaches the failing row as a note |
| `GeometryError: values contains missing geometries` | a list/sequence was used where the operation requires dense geometry values | call [`drop_missing()`][gometry.GeometryArray.drop_missing] first or [`fill_missing(...)`][gometry.GeometryArray.fill_missing] with an explicit replacement |
| `GeometryError: ... must be between ... got ...` | a parameter value is out of its documented range | the message names the kwarg and the valid range |
| `OverflowError: <operation> cannot be represented in binary64` | no adjacent-coordinate assignment passed the exact output certificate | simplify or snap near-coincident input boundaries when the application allows it, or split the operation |
| `ResourceLimitError: <operation> could not certify ... within its search budget` | the bounded search stopped before certifying an assignment; this does not prove that no assignment exists | simplify or snap near-coincident input boundaries when the application allows it, or split the operation |
| `GeometryError: unknown ... did you mean ...?` | a token typo (`'mitter'` vs `'miter'`, …) | take the suggestion; tokens are also `Literal`-typed, so a type checker catches this before runtime |
| `InvalidGeometryError: invalid longitude/latitude (...)` | geographic input outside the CRS-native longitude period or latitude/pole domain | check axis order — gometry is always `(x, y)`; under a geographic CRS that is `(lon, lat)` in native angular units; [`swap_xy()`][gometry.Geometry.swap_xy] repairs latitude-first data |
| `ambiguous_full_turn_ring` in a geographic validation report | an unmarked ring winds once around the longitude period but its latitude span crosses the equator or lies entirely on it | close the boundary explicitly through a pole or split it into unambiguous rings |
| metric looks unexpectedly large or small | raw angular coordinates were mistaken for a metric unit | let the CRS drive (no `unit=`), and check the geometry carries the intended CRS |

## Error attributes

Operation-raised errors carry operation values as attributes, so handlers act on
data instead of parsing messages. Gometry classes provide `None` defaults when
an attribute does not apply. Built-in `OverflowError` instances carry `code` and
`operation` only for certified output refusals; use `getattr(error, 'code', None)`
when handling other overflow sources too.

| Attribute | Classes | Meaning |
|---|---|---|
| [`field`][gometry.CRSMismatchError.field] | [`CRSMismatchError`][gometry.CRSMismatchError] | Which metadata disagreed: `'crs'` or `'epoch'` |
| [`left`][gometry.CRSMismatchError.left] / [`right`][gometry.CRSMismatchError.right] | `CRSMismatchError` | Raw conflicting values (CRS identifiers or epoch floats; not quoted presentation strings) |
| [`index`][gometry.CRSMismatchError.index] | `CRSMismatchError` | Collection item that disagreed (when applicable) |
| [`format`][gometry.ParseError.format] | [`ParseError`][gometry.ParseError] | Lowercase space-free codec key, including `'pickle'` alongside `'wkt'`, `'wkb'`, `'geojson'`, `'geoarrow'`, `'geoparquet'`, grid codecs, and geocoders |
| [`position`][gometry.ParseError.position] | `ParseError` | WKT/EWKT reports the zero-based UTF-8 byte offset where parsing detected the error, including an EWKT prefix; WKB reports its zero-based binary byte offset; other codecs report `None` when unavailable |
| [`param`][gometry.GeometryError.param] / [`value`][gometry.GeometryError.value] | [`GeometryError`][gometry.GeometryError] (value lanes) | Offending keyword and value |
| [`parameter`][gometry.ResourceLimitError.parameter] / [`produced`][gometry.ResourceLimitError.produced] / [`limit`][gometry.ResourceLimitError.limit] | bounded-output `ResourceLimitError` | Controlling parameter, prospective count (or proven lower bound), and configured limit |
| [`code`][gometry.ResourceLimitError.code] | `ResourceLimitError` | `'output_limit'` or `'work_limit'`; `None` for hand-built errors |
| `code` / `operation` | certified-output `OverflowError` | `'output_unrepresentable'` and the operation that could not publish its result |
| [`expected`][gometry.GeometryTypeError.expected] / [`got`][gometry.GeometryTypeError.got] | [`GeometryTypeError`][gometry.GeometryTypeError] | Required and received geometry kinds |
| [`source`][gometry.TransformError.source] / [`target`][gometry.TransformError.target] | [`TransformError`][gometry.TransformError] | Source and target CRS of a failed transform |
| [`operation`][gometry.ResourceLimitError.operation] / [`operation`][gometry.InvalidGeometryError.operation] | `ResourceLimitError`; `InvalidGeometryError` (empty sequence or invalid coverage) | Operation associated with the failure |
| [`row`][gometry.InvalidGeometryError.row] / [`path`][gometry.InvalidGeometryError.path] | [`InvalidGeometryError`][gometry.InvalidGeometryError] (require) | Failing array row and structural path |
| [`code`][gometry.InvalidGeometryError.code] / [`detail`][gometry.InvalidGeometryError.detail] | [`InvalidGeometryError`][gometry.InvalidGeometryError] (require) | Open-string validity issue code and human-readable issue detail |
| [`location`][gometry.InvalidGeometryError.location] | [`InvalidGeometryError`][gometry.InvalidGeometryError] (require) | Coordinate witness for the validity issue; quotient-topology issues choose a canonical coordinate of their contact set deterministically by minimum latitude then longitude |
| [`crs`][gometry.CRSError.crs] | [`CRSError`][gometry.CRSError] | CRS involved in a unit/authority mismatch |

```python exec="on" source="block" result="text"
import gometry as gm

try:
    gm.contains(gm.box(0, 0, 1, 1), gm.Point(1, 1, crs=4326))
except gm.CRSMismatchError as e:
    print('field:', e.field, '| left:', e.left, '| right:', e.right)

try:
    gm.from_wkt('POINT (oops)')
except gm.ParseError as e:
    print('format:', e.format)

```

## Rows in array errors

A per-element failure in an array operation keeps its class and message —
so `except ParseError` still catches — and names the failing row as a
[PEP 678](https://peps.python.org/pep-0678/) note that tracebacks display:

```python exec="on" source="block" result="text"
import gometry as gm

try:
    gm.from_wkt(['POINT (1 2)', 'POINT (oops)'])
except gm.ParseError as e:
    print(e)
    print(e.__notes__[0])

```

<a id="message-grammar"></a>

Branch on exception classes, attributes, and `.code` values rather than
parsing error messages.

## Pickling

Exceptions cross process boundaries intact — `multiprocessing`, `dask`, and
`concurrent.futures` re-raise them as the same class with the same message:

```python exec="on" source="block" result="text"
import pickle

import gometry as gm

error = gm.GeometryError('h3 resolution must be between 0 and 15, got 99')
clone = pickle.loads(pickle.dumps(error))
print(type(clone).__name__, '|', clone)

```

## See also

- [Validation & repair](validation.md) — turning invalid geometry into a report, then a fix.
- [CRS, units & measurement](crs.md) — the frame rules behind [`CRSMismatchError`][gometry.CRSMismatchError].
- [Security & untrusted input](../about/security.md) — catching [`ParseError`][gometry.ParseError] at trust boundaries.
- [API: errors](../api/errors.md) — the full class reference.
