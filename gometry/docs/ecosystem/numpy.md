---
description: NumPy interchange in gometry — read-only typed ndarrays for every bulk result, zero-copy coordinate columns, the buffer protocol, and SciPy handoffs.
---

# NumPy arrays & coordinates

NumPy is the one **required** runtime dependency.
Geometry containers in Shapely and GeoPandas represent geometry objects rather
than pretending they are numeric dtypes. For gometry's *numeric bulk results*,
gometry returns fixed-width **typed `numpy.ndarray`s** straight from Rust — `float64` for
metrics and coordinates, `bool_` for predicate masks, `int64` for ids and labels,
`uint64` for grid-cell ids and space-filling-curve keys. No per-element Python
objects, no hidden conversions.

## Every dense numeric bulk result is a typed ndarray

```python exec="on" source="block" result="text"
import numpy as np
import gometry as gm

zones = gm.GeometryArray([gm.box(i, 0, i + 2, 2) for i in range(4)])

areas = zones.area                                   # float64 ndarray
mask = gm.intersects(zones, gm.box(1, 1, 3, 3))    # bool_ ndarray
left, right = gm.SpatialIndex(zones).self_join()         # (left, right) int64 ndarrays

print("areas:", areas.dtype, areas)
print("mask :", mask.dtype, mask)
print("pairs:", left.dtype, list(zip(left.tolist(), right.tolist())))
```

The arrays come back read-only and already in the right dtype, so
`numpy.asarray(result)` is a **zero-copy view** when the dtype matches and
`.tolist()` returns plain Python lists. Per-row bounds are `(rows, 4)`
`float64` matrices (`nan` rows for empty geometries); array index queries return
[`Groups`][gometry.Groups], a CSR wrapper whose [`.values`][gometry.Groups.values]/[`.offsets`][gometry.Groups.offsets] and
per-row views are `int64` ndarrays. Text outputs ([`to_wkt`][gometry.Geometry.to_wkt], geohash tokens), byte
outputs ([`to_wkb`][gometry.Geometry.to_wkb]), and ragged diagnostics stay Python lists because they have no
single dense ndarray shape.

## Coordinate columns, zero-copy

[`geom.coords`][gometry.Coordinates] exposes per-axis columns directly. [`.x`][gometry.Coordinates.x]/[`.y`][gometry.Coordinates.y]
are always read-only `float64` ndarrays; [`.z`][gometry.Coordinates.z]/[`.m`][gometry.Coordinates.m] appear when any coordinate
carries that ordinate, with `NaN` for rows that lack it on a mixed-dimension
[`GeometryArray`][gometry.GeometryArray].

```python exec="on" source="block" result="text"
import numpy as np
import gometry as gm

line = gm.LineString([(0.0, 0.0), (3.0, 4.0), (6.0, 8.0)])
xs = line.coords.x                 # read-only float64 view
print("x column:", xs, "| shares storage:", np.asarray(xs).base is not None)

# Bulk extraction across a whole array, with the source-row index:
arr = gm.GeometryArray([gm.Point(1, 2), gm.LineString([(0, 0), (1, 1)])])
coords, index = gm.get_coordinates(arr, return_index=True)
print("coords:\n", coords)
print("source row per vertex:", index)
```

`coords.x` shares storage; `np.asarray(coords)` materializes a dense
`(N, dims)` matrix (a copy, since storage is column-major) with `NaN`
for absent Z/M; [`coords.to_dict()`][gometry.Coordinates.to_dict] returns read-only ndarray columns such as
`{'x': ndarray, 'y': ndarray}` for direct pandas/Polars construction (forced
Z/M columns use `NaN` where absent). [`get_coordinates`][gometry.get_coordinates] takes
`axes=` / `return_index=` for bulk extraction across a geometry
or array.

## The geometry ↔ object-array bridge

[`GeometryArray.to_numpy`][gometry.GeometryArray.to_numpy] is the explicit escape
hatch to an `object` ndarray of geometry handles — there is no silent path that
builds slow object arrays behind your back. The inverse is the
[`GeometryArray`][gometry.GeometryArray] constructor, which accepts an object
ndarray (or any iterable) of gometry geometries **or** `__geo_interface__` objects,
including Shapely geometries.

[`Geometry`][gometry.Geometry], [`GeometryArray`][gometry.GeometryArray], and [`Coordinates`][gometry.Coordinates] set `__array_ufunc__ = None`, so
NumPy ufuncs defer with a clear `TypeError` instead of forcing an implicit
conversion. `GeometryArray` and `Coordinates` implement `__array__`, so
`np.asarray(arr)` gives an `object` array and `np.asarray(coords)` gives an
`(N, dims)` `float64` matrix.

## Cell arrays

[`CellArray`][gometry.CellArray] exposes H3, S2, and tile ids as `uint64`
through [`.to_numpy()`][gometry.CellArray.to_numpy] (zero-copy when contiguous) and NumPy's array protocol.
For those numeric grids, `np.asarray(cells)` gives raw ids, while
`np.asarray(cells, dtype=object)` gives typed cell objects. Geohash's public
identity is its base-32 string token, not an implementation integer, so
`geohash_cells.to_numpy()` and `np.asarray(geohash_cells)` return a read-only
object array of [`GeohashCell`][gometry.GeohashCell] values; use [`.token`][gometry.Cell.token] for strings.

Cell arrays with missing rows are logical sequences of [`Cell | None`][gometry.Cell]. Their
default NumPy export is a read-only object array containing the typed cells and
`None`; `dtype=object` always has this behavior. Explicit `dtype=uint64` is
rejected for a masked array, and `copy=False` is rejected whenever an object,
masked, or gathered export would need materialization. A selection containing
only present rows can use the ordinary zero-copy numeric export.

```python exec="on" source="block" result="text"
import numpy as np
import gometry as gm

cells = gm.h3_cells([21.0, 22.0], [52.0, 52.5], resolution=7)  # CellArray[H3Cell]
ids = cells.to_numpy()             # uint64 ndarray, zero-copy view
print("dtype:", ids.dtype, "| n:", ids.size)
print("object:", type(np.asarray(cells, dtype=object)[0]).__name__)
```

## Handing off to SciPy and scikit-learn

Scattered-data interpolation (IDW, kriging, natural-neighbor) and coordinate
clustering (k-means, DBSCAN, hierarchical) live in SciPy, scikit-gstat, and
scikit-learn; gometry supplies their coordinate input.
Pull a `float64` matrix with [`get_coordinates`][gometry.get_coordinates]
(or per-axis [`.coords.x`][gometry.Coordinates.x]/[`.y`][gometry.Coordinates.y]) and pass it straight in. Project to a metric CRS
first when planar XY is wrong for geographic data:

```python exec="on" source="block" result="text"
import gometry as gm
from sklearn.cluster import KMeans          # not a gometry dependency

# two spatial clusters (west vs east), not one point per cluster
lons = [13.35, 13.36, 13.37, 13.50, 13.51, 13.52]
lats = [52.48, 52.49, 52.50, 52.51, 52.52, 52.53]
pts = gm.points(lons, lats, crs=4326)
xy = gm.get_coordinates(pts.to_crs(pts.estimate_local_crs()))   # planar meters
labels = KMeans(n_clusters=2, n_init=10, random_state=0).fit_predict(xy)
print("labels:", labels.tolist())
```

The same pattern feeds SciPy's `cKDTree`, `cdist`, and interpolators — the ndarray
is the contract, so anything in the scientific stack consumes it directly.

## Python Array API compatibility

gometry implements the NumPy interchange protocols (`__array__`,
`__array_ufunc__ = None`, and the buffer protocol) and returns typed ndarrays, but
it is **not** a [Python Array API](https://data-apis.org/array-api/) provider and
does not expose `__array_namespace__`. The Array API standard targets numeric
array libraries for elementwise math and linear algebra; gometry arrays hold
geometries or cells, not numbers. Use gometry to produce coordinate arrays, then
pass them to NumPy, SciPy, or scikit-learn.

## See also

- [Arrow & storage](arrow.md) — columnar, zero-copy interchange beyond raw coordinates.
- [DataFrames](dataframes.md) — store geometry columns in pandas/polars.
- [Arrays & performance](../guide/arrays.md) — the [`GeometryArray`][gometry.GeometryArray]/[`CellArray`][gometry.CellArray]/[`Groups`][gometry.Groups] model.
