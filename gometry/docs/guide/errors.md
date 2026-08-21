---
description: gometry's exception hierarchy — catching errors, error attributes, per-row context in array operations, message grammar, and pickling.
---

# Errors & exceptions

gometry exposes a typed exception hierarchy rooted at [`GeometryError`][gometry.GeometryError] rather than
using generic `ValueError`s for every domain failure. Every class is available
directly from `gometry`, such as
[`gm.CRSMismatchError`][gometry.CRSMismatchError].

```text
ValueError
└── GeometryError              every error gometry raises about your data or parameters
    ├── InvalidGeometryError        a geometry violates a structural or numeric rule
    ├── GeometryTypeError    wrong geometry kind (also subclasses TypeError)
    ├── CRSError             a CRS could not be created, identified, exported, or used
    │   ├── CRSMismatchError operands carry incompatible CRS / coordinate-epoch metadata
    │   └── TransformError   a coordinate transform could not be built or failed to run
    └── ParseError           serialized input (WKT, WKB, GeoJSON, cell ids/tokens) is malformed
```

Grid-cell surfaces follow the same split as everything else: an out-of-range
`resolution`/`level`/`precision`/`zoom` is a plain `GeometryError` (a bad
parameter value), while an invalid cell id, token, or quadkey is a
[`ParseError`][gometry.ParseError] whose `.format` tag names the system (`'h3'`, `'s2'`,
`'geohash'`, `'tile'`, `'quadkey'`); codec tags are lowercase machine keys.

## Built-in exception compatibility

1. **Domain errors are also `ValueError`s.** The base class subclasses
   `ValueError` (the `json.JSONDecodeError` precedent), so a broad
   `except ValueError` catches them while specific classes preserve recovery detail.
2. **Python protocol semantics stay builtin.** A non-number where a number is
   required is a plain `TypeError` (floats *and* integers); an out-of-range
   `arr[i]` is an `IndexError`; an exhausted iterator is `StopIteration`.
   Those protocol failures are not rewrapped.

[`GeometryTypeError`][gometry.GeometryTypeError] is a dual-base exception: handing
[`line_substring`][gometry.Geometry.line_substring] a [`Polygon`][gometry.Polygon] is *both* a gometry domain error and a Python type
error, so it subclasses both [`GeometryError`][gometry.GeometryError] and `TypeError` (so either `except`
clause catches it). That is the one place gometry subclasses `TypeError` —
wrong-kind domain failures, not protocol coercion.

## Catching errors

The most specific exception class identifies the recovery strategy:

| I want to… | catch |
|---|---|
| reject a malformed scalar or batch ingest | [`ParseError`][gometry.ParseError] |
| reproject and retry on frame disagreements | [`CRSMismatchError`][gometry.CRSMismatchError] |
| fall back when a transform cannot be built or run | [`TransformError`][gometry.TransformError] |
| handle any CRS trouble in one place | [`CRSError`][gometry.CRSError] |
| reject anything gometry complained about | [`GeometryError`][gometry.GeometryError] |
| treat a wrong geometry kind as a type bug | `TypeError` *(or [`GeometryTypeError`][gometry.GeometryTypeError])* |
| treat any bad value like the stdlib does | `ValueError` |


```python exec="on" source="block" result="text"
import gometry as gm

square = gm.box(0, 0, 2, 2)                 # CRS-free
tagged = gm.Point(1, 1, crs=4326)

try:
    gm.contains(square, tagged)
except gm.CRSMismatchError as e:
    print(type(e).__name__, '->', e)

```

A batch parser is fail-fast: one malformed row rejects the whole batch and the
exception identifies the row. If partial recovery is an explicit application
policy, parse rows individually and catch `ParseError`; a tool that wants
"anything gometry rejected" catches `GeometryError`:

```python exec="on" source="block" result="text"
import gometry as gm

rows = ['POINT (1 2)', 'POINT (oops)', 'LINESTRING (0 0, 1 1)']
parsed = []
for row in rows:
    try:
        parsed.append(gm.from_wkt(row))
    except gm.ParseError:
        pass  # skip malformed rows
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
| `CRSMismatchError: ... CRS-free vs "EPSG:4326"` | one operand carries a CRS and the other doesn't (or they differ) | tag the bare one with `set_crs(...)`, or reproject with `to_crs(...)` — gometry never coerces frames |
| `CRSError: set_crs would re-tag CRS ... without moving coordinates` | you asked to *relabel* a geometry that already has a CRS | `to_crs(...)` to reproject; `set_crs(..., overwrite=True)` only if the original tag was wrong |
| `CRSError: ... epoch requires a CRS` | a coordinate epoch is meaningless without a frame | set the CRS first (or together: `Point(..., crs=..., epoch=...)`) |
| `InvalidGeometryError: ... cannot carry Z/M` (`to_geojson` on M or `to_polyline` on Z/M) | the target format has no slot for those ordinates | use WKB/WKT/GeoArrow, or explicitly clear dimensions with `set_m(None)` / `force_2d()` when loss is intended |
| [`ParseError`][gometry.ParseError] with `.format` set | a serialized input (WKT/WKB/GeoJSON/cell id/quadkey) is malformed | check `e.format` for which codec rejected it; a batch fails as a unit and attaches the failing row as a note |
| `GeometryError: values contains missing geometries` | a list/sequence was used where the operation requires dense geometry values | call [`drop_missing()`][gometry.GeometryArray.drop_missing] first or [`fill_missing(...)`][gometry.GeometryArray.fill_missing] with an explicit replacement |
| `GeometryError: ... must be between ... got ...` | a parameter value is out of its documented range | the message names the kwarg and the valid range |
| `GeometryError: unknown ... did you mean ...?` | a token typo (`'mitter'` vs `'miter'`, …) | take the suggestion; tokens are also `Literal`-typed, so a type checker catches this before runtime |
| `InvalidGeometryError: invalid longitude/latitude (...)` | geographic input outside ±180/±90 | check axis order — gometry is always `(x, y)`; under a geographic CRS that is `(lon, lat)`; [`swap_xy()`][gometry.Geometry.swap_xy] repairs latitude-first data |
| metric looks unexpectedly large or small | degrees treated as meters somewhere | let the CRS drive (no `unit=`), and check the geometry carries the intended CRS |

## Error attributes

Operation-raised errors carry operation values as attributes, so
handlers act on data instead of parsing messages. Class-level defaults are
`None` on instances from a different lane (hand-constructed instances too):

| Attribute | Classes | Meaning |
|---|---|---|
| [`field`][gometry.CRSMismatchError.field] | [`CRSMismatchError`][gometry.CRSMismatchError] | Which metadata disagreed: `'crs'` or `'epoch'` |
| [`left`][gometry.CRSMismatchError.left] / [`right`][gometry.CRSMismatchError.right] | `CRSMismatchError` | Raw conflicting values (CRS identifiers or epoch floats; not quoted presentation strings) |
| [`index`][gometry.CRSMismatchError.index] | `CRSMismatchError` | Collection item that disagreed (when applicable) |
| [`format`][gometry.ParseError.format] | [`ParseError`][gometry.ParseError] | Lowercase space-free codec key, including `'pickle'` alongside `'wkt'`, `'wkb'`, `'geojson'`, `'geoarrow'`, `'geoparquet'`, grid codecs, and geocoders |
| [`position`][gometry.ParseError.position] | `ParseError` | WKT reports the UTF-8 input length for every failure; cursor-based codecs such as WKB report the byte offset where parsing detected it |
| [`param`][gometry.GeometryError.param] / [`value`][gometry.GeometryError.value] | [`GeometryError`][gometry.GeometryError] (value lanes) | Offending keyword and value |
| [`parameter`][gometry.GeometryError.parameter] / [`produced`][gometry.GeometryError.produced] / [`limit`][gometry.GeometryError.limit] | bounded-output `GeometryError` | Controlling parameter, produced count, and configured limit |
| [`expected`][gometry.GeometryTypeError.expected] / [`got`][gometry.GeometryTypeError.got] | [`GeometryTypeError`][gometry.GeometryTypeError] | Required and received geometry kinds |
| [`source`][gometry.TransformError.source] / [`target`][gometry.TransformError.target] | [`TransformError`][gometry.TransformError] | Source and target CRS of a failed transform |
| [`operation`][gometry.InvalidGeometryError.operation] | [`InvalidGeometryError`][gometry.InvalidGeometryError] (overlay) | Overlay op name |
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
so [`except ParseError`][gometry.ParseError] still catches — and names the failing row as a
[PEP 678](https://peps.python.org/pep-0678/) note that tracebacks display:

```python exec="on" source="block" result="text"
import gometry as gm

try:
    gm.from_wkt(['POINT (1 2)', 'POINT (oops)'])
except gm.ParseError as e:
    print(e)
    print(e.__notes__[0])

```

## Message grammar

Every message follows one voice — lowercase start, present tense, the exact
public keyword names, `repr` for user-supplied strings, no trailing period:

- `<subject> must <constraint>, got <value>` — `tolerance must be a non-negative finite number, got -1`
- `<operation> requires <condition>` — `split requires a Point or MultiPoint splitter`
- `invalid <FORMAT>: <detail>` — `invalid WKT: unbalanced parentheses`
- `unknown <concept> <value>; expected …` — `unknown buffer cap_style "fancy"; expected 'round', 'flat', or 'square'` — with a `did you mean '<closest>'?` clause when the value looks like a typo

Callers can branch on exception classes, structured attributes, and message
shapes.

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
