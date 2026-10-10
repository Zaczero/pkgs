---
description: NumPy interchange in gometry — read-only typed ndarrays for every bulk result, accepted PEP-3118 input buffers, zero-copy coordinate columns, and SciPy handoffs.
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

areas = zones.area
mask = gm.intersects(zones, gm.box(0.5, 1, 1.5, 3))
left, right = gm.SpatialIndex(zones).self_join()

print("areas:", areas.dtype, repr(areas))
print("mask:", mask.dtype, repr(mask))
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

Numeric input columns also accept PEP-3118 buffers when the operation accepts a
`FloatColumn` or integer column. This is input admission, not an output-buffer
promise: gometry does not export geometry containers as PEP-3118 numeric buffers.
Input buffers are copied into native storage. Capture is synchronous: keep the
producer quiescent (do not modify it) until the call returns. Holding a buffer
view keeps its allocation alive but does not synchronize a native writer or
make a torn concurrent read coherent. Once capture returns, later mutations of
the input cannot affect the completed geometry. The native coordinate
representation is float64, so a float32 `FloatColumn` is accepted but converted
to float64 during ingestion; `FloatColumn` input is not zero-copy.

## Coordinate views and matrix exports

[`geom.coords`][gometry.Coordinates] exposes per-axis columns directly. [`.x`][gometry.Coordinates.x]/[`.y`][gometry.Coordinates.y]
are always read-only `float64` ndarrays. [`.z`][gometry.Coordinates.z]/[`.m`][gometry.Coordinates.m]
are also always read-only `float64` ndarrays of the view length; they contain
`NaN` when no coordinate carries that ordinate, and mixed-dimension rows use
`NaN` for the rows that lack it.

```python exec="on" source="block" result="text"
import numpy as np
import gometry as gm

line = gm.LineString([(0.0, 0.0), (3.0, 4.0), (6.0, 8.0)])
xs = line.coords.x                 # read-only float64 column
print("x column:", xs.tolist(), "| read-only:", not xs.flags.writeable)

arr = gm.GeometryArray([gm.Point(1, 2), gm.LineString([(0, 0), (1, 1)])])
coords = arr.coords
matrix = np.asarray(coords, dtype=np.float64)
row_index = coords.row_index
print("coords:\n", matrix.tolist())
print("source-row index:", row_index.tolist())
```

Per-axis columns can borrow storage when the carried layout permits it;
`np.asarray(coords, dtype=np.float64)` materializes a dense
`(N, dims)` matrix and always copies, with `NaN` for absent Z/M.
[`coords.to_dict()`][gometry.Coordinates.to_dict] returns
read-only ndarray columns such as `{'x': ndarray, 'y': ndarray}` for direct
Pandas/Polars construction. It omits Z/M when the view does not carry those
axes; mixed views include their union and use `NaN` where a row lacks an axis.
Use `coords.select('XY')`, `coords.select('XYZ')`, `coords.select('XYM')`, or
`coords.select('XYZM')` to choose a fixed coordinate-view layout. The selected
layout changes nested output, iteration, ndarray/dict exports, and equality
without changing the geometry. For a flattened `GeometryArray.coords` view, use
[`.row_index`][gometry.Coordinates.row_index] to map each coordinate back to
its source geometry row. The matrix includes every carried axis; use
`coords.select('XY')` before conversion when a fixed layout is needed. For a
foreign iterable, first use `gm.GeometryArray(values).coords`; this admits the
same row formats as the array constructor.

## Coordinate matrices

[`Geometry.with_coordinates`][gometry.Geometry.with_coordinates] and
[`GeometryArray.with_coordinates`][gometry.GeometryArray.with_coordinates] accept
one rank-2 coordinate matrix. Its shape is `(N, 2)` for XY, `(N, 3)` for XYZ
or XYM, or `(N, 4)` for XYZM, and `N` must match the receiver's coordinate
count (or the flattened present-row count for an array). The matrix width must
match the receiver's axes. A rank other than 2, an unsupported width, or a
row-count mismatch raises an error. The matrix is converted to native float64
values, so this is a validated replacement input rather than a zero-copy view.
Keyword columns can instead replace selected ordinates; their scalar-broadcast,
flattened ordering, omission, and dimension-removal rules are described in
[Z and M under operations](../guide/geometry.md#zm-under-operations).

## The geometry ↔ object-array bridge

[`GeometryArray.to_numpy`][gometry.GeometryArray.to_numpy] explicitly exports an
`object` ndarray of geometry handles. NumPy's array protocol also supports
`np.asarray(arr)` and `np.asarray(arr, dtype=object)`, which materialize an object
array when requested. The inverse is the [`GeometryArray`][gometry.GeometryArray]
constructor, which accepts an object
ndarray (or any iterable) of gometry geometries **or** `__geo_interface__` objects,
including Shapely geometries.

[`Geometry`][gometry.Geometry] and `GeometryArray` set `__array_ufunc__ = None`, so
NumPy ufuncs defer with a clear `TypeError` instead of assigning numeric meaning
to spatial values. `GeometryArray` and `Coordinates` implement `__array__`, so
`np.asarray(arr)` gives an `object` array and `np.asarray(coords)` gives an
`(N, dims)` `float64` matrix. Coordinates therefore work directly with numeric
ufuncs; the resulting arrays remain read-only where coordinate export is
read-only, including as a ufunc `out=` target.

Integer-array selection preserves known NumPy rank in its result type: a
zero-dimensional integer array selects one row, while a one-dimensional integer
array selects a container; selectors with rank greater than one raise
`TypeError`. An ndarray annotation with a variable-length tuple of integer
dimensions gives a scalar-or-container result; when NumPy erases the shape to
`Any`, annotate the rank for a narrower result. See
[array indexing](../guide/arrays.md#indexing-slicing-and-element-types).

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

cells = gm.h3_cells([21.0, 22.0], [52.0, 52.5], resolution=7)
ids = cells.to_numpy()             # zero-copy ndarray
print("dtype:", ids.dtype, "| n:", ids.size)
print("object:", type(np.asarray(cells, dtype=object)[0]).__name__)
```

## Handing off to SciPy and scikit-learn

Scattered-data interpolation (IDW, kriging, natural-neighbor) and coordinate
clustering (k-means, DBSCAN, hierarchical) live in SciPy, scikit-gstat, and
scikit-learn; gometry supplies their coordinate input.
Pull a `float64` matrix with `np.asarray(geom.coords, dtype=np.float64)`
(or use per-axis `.coords.x`/`.y`) and pass it straight in. Project to a metric CRS
first when planar XY is wrong for geographic data:

Install scikit-learn for the clustering example:

=== "uv"

    ```bash
    uv add scikit-learn
    ```

=== "pip"

    ```bash
    python -m pip install scikit-learn
    ```

```python exec="on" source="block" result="text"
import numpy as np
import gometry as gm
from sklearn.cluster import KMeans          # not a gometry dependency

lons = [13.35, 13.36, 13.37, 13.50, 13.51, 13.52]
lats = [52.48, 52.49, 52.50, 52.51, 52.52, 52.53]
pts = gm.points(lons, lats, crs=4326)
xy = np.asarray(pts.to_crs(pts.estimate_local_crs()).coords, dtype=np.float64)  # planar meters
labels = KMeans(n_clusters=2, n_init=10, random_state=0).fit_predict(xy)
print("labels:", labels.tolist())
```

The same pattern feeds SciPy's `cKDTree`, `cdist`, and interpolators — the ndarray
is the contract, so anything in the scientific stack consumes it directly.

## Python Array API compatibility

gometry implements the NumPy interchange protocols (`__array__` and
`__array_ufunc__ = None`) and returns typed ndarrays. Numeric input columns can
consume PEP-3118 buffers, but geometry and cell containers do not export numeric
PEP-3118 buffers. gometry is **not** a [Python Array API](https://data-apis.org/array-api/)
provider and does not expose `__array_namespace__`. The Array API standard
targets numeric array libraries for elementwise math and linear algebra; gometry
arrays hold geometries or cells, not numbers. Use gometry to produce coordinate
arrays, then pass them to NumPy, SciPy, or scikit-learn.

## See also

- [Arrow & storage](arrow.md) — columnar, zero-copy interchange beyond raw coordinates.
- [DataFrames](dataframes.md) — store geometry columns in pandas/polars.
- [Arrays & performance](../guide/arrays.md) — the `GeometryArray`/`CellArray`/`Groups` model.
