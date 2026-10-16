---
description: How gometry names operations and represents geometry, arrays, units, and errors.
---

# Design principles

gometry uses a few naming conventions so you can predict how an unfamiliar
operation works.

## API design rules

### 1. Place work on the value or relationship it describes

Facts are properties, unary operations are methods, and binary relationships
are free functions:

```python exec="on" source="block" result="text"
import gometry as gm

point = gm.Point(1, 2)
print(point.area)
print(point.buffer(1).geometry_type)
print(gm.distance(point, gm.Point(4, 6)))
```

Without a CRS, distances and buffers use raw coordinate units.

<a id="copy-returning-replacements"></a>

### 2. Name operations by their effect on immutable values

`with_<field>` returns a changed copy, `to_<format>` serializes, and
`from_<format>` parses. [`with_crs`][gometry.Geometry.with_crs] assigns metadata;
[`to_crs`][gometry.Geometry.to_crs] transforms coordinates.
[`force_2d`][gometry.Geometry.force_2d] and
[`force_3d`][gometry.Geometry.force_3d] change coordinate dimensions.

### 3. Use `_all` for pooled many-to-one operations

[`union_all`][gometry.union_all] combines an entire sequence.
Array methods operate row by row: `arr.polygonize()` polygonizes each row,
while [`polygonize_all(arr)`][gometry.polygonize_all] pools their linework
into one graph.

### 4. Return `Groups` when each input row can produce a different number of results

[`Groups`][gometry.Groups] preserves the association between source rows and
ragged results. A missing row is `None`; a present row with no results is an
empty group.

### 5. Select an operation's representation or added output with a typed keyword

Options such as [`CRS.to_projjson(as_dict=True)`][gometry.CRS.to_projjson]
keep related outputs under one name. Type annotations narrow the result
when you choose an option explicitly.

<a id="scalar-and-vectorized-forms"></a>

### 6. Keep supported input forms under one operation name

For example, [`from_wkt`][gometry.from_wkt] accepts one string or a sequence.
Binary operations broadcast a scalar against an array, or pair equal-length
arrays row by row. Unequal array lengths are an error.

### 7. Preserve known facts in public types

Annotations track geometry kind, missing values, and scalar versus array
results. A point's buffer is a polygon; buffering an array with missing
rows preserves those missing rows.

<a id="crs-and-candidate-boundaries"></a>

### 8. Prefer one obvious way, with exceptions for distinct capabilities

[`SpatialIndex.candidates`][gometry.SpatialIndex.candidates] returns bounding-box
matches; [`query`][gometry.SpatialIndex.query] refines them with an exact
predicate. Grid covers likewise select cells, not exact geometry membership.
Use a predicate on the source geometry when membership matters.

### 9. Treat Python and ecosystem protocols as public API

Geometry values support `a & b`, `str(geom)`, formatting, pattern matching,
hashing, and equality. Arrays support indexing, slicing, and iteration.
[`NumPy`](../ecosystem/numpy.md), [Arrow](../ecosystem/arrow.md), and
[dataframe adapters](../ecosystem/dataframes.md) provide bulk interchange.

## API structure and domain conventions

Classes construct individual values; plural functions construct arrays:
[`Point`][gometry.Point] and [`points`][gometry.points],
[`LineString`][gometry.LineString] and [`line_strings`][gometry.line_strings].

Grid functions use `h3_`, `s2_`, `geohash_`, and `tile_` prefixes.
Their granularity parameters retain the familiar domain names:
`resolution`, `level`, `precision`, and `zoom`, respectively.

## Canonical entry points

The [migration cheatsheet](../migrating/cheatsheet.md) maps familiar Shapely,
pyproj, H3, S2, and rtree operations to gometry.

For comparisons, choose the meaning you need:

- [`equals`][gometry.equals]: the same occupied point set.
- [`equals_exact`][gometry.equals_exact]: coordinates within a tolerance,
  in compatible frames.
- [`equals_identical`][gometry.equals_identical]: the same coordinates, CRS,
  and epoch. Python `==` uses this value equality.

`GeometryArray == GeometryArray` compares whole arrays and returns one bool.
Use `equals_identical(left, right)` for elementwise comparison.
For CRS values, `==` compares stored definitions;
[`CRS.same_as`][gometry.CRS.same_as] tests operational equivalence.

## NumPy-native bulk outputs

Dense numeric results are read-only NumPy arrays, geometry results are
[`GeometryArray`][gometry.GeometryArray], and ragged results are
[`Groups`][gometry.Groups]. Text and diagnostics use lists where no dense
shape fits. See [Arrays & performance](../guide/arrays.md).

## Errors as API

Geometry errors derive from [`GeometryError`][gometry.GeometryError], itself
a `ValueError`. More specific exceptions let you distinguish malformed input,
CRS conflicts, transformation failures, and invalid geometry.
Ordinary Python protocol errors retain their built-in types.
See [Errors & exceptions](../guide/errors.md).

## Z and M layouts

XY, XYZ, XYM, and XYZM are supported. Whether an operation preserves, computes,
or drops Z/M depends on the operation; see
[Geometry & dimensions](../guide/geometry.md#zm-under-operations).
Interchange formats also differ: WKT/WKB preserve M but not coordinate epochs,
whereas GeoArrow can preserve both. [Text & binary formats](../ecosystem/text-formats.md)
explains the choices.

## Overlay linework and point-set presentation

Overlay joins linework through degree-two nodes and splits at genuine
junctions. A line clipped by a covering polygon therefore remains one line.
[`line_merge`][gometry.Geometry.line_merge] applies the same rule to
independent linework.

## The Arrow / GeoArrow boundary

Homogeneous geometry arrays use GeoArrow-compatible coordinate buffers.
Mixed types, collections, or layouts without a native representation use WKB
fallback. Dataframes and databases remain separate from the geometry engine;
see [Arrow & storage](../ecosystem/arrow.md).

<a id="reliability-and-supply-chain-posture"></a>

For parsing and resource limits, see [Security & untrusted input](security.md).
Bundled-component notices are covered by [License](license.md).
