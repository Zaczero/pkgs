---
description: gometry public API design rules for operation placement, immutable values, pooled results, result types, Python protocols, and ecosystem integration.
---

# Design principles

gometry's public API follows nine rules. They explain where operations live,
how inputs and results are named and typed, and how geometry values integrate
with Python and its data ecosystem.

## API design rules

### 1. Place work on the value or relationship it describes

Facts about one value are properties, unary geometry work is a method, and
relationships between two geometries are free functions. This makes an
operation's owner predictable. See the [`Geometry.area` property][gometry.Geometry.area],
[`Geometry.buffer` method][gometry.Geometry.buffer], and
[`distance` function][gometry.distance].

```python exec="on" source="block" result="text"
import gometry as gm

point = gm.Point(1, 2)
print(point.area, type(point.buffer(1)).__name__, gm.distance(point, gm.Point(4, 6)))

```

The example has no CRS, so its distance and buffer use raw coordinate units.

### 2. Name operations by their effect on immutable values

Copy-returning state replacements use `with_<field>` names. `to_<x>` names
transform or serialize, `from_<x>` names parse, and `force_<dim>` names project
coordinate dimensions. The name tells callers whether an operation constructs
from an encoding, transforms a value, or returns a changed value. See
[`Geometry.to_crs`][gometry.Geometry.to_crs],
[`Geometry.to_wkt`][gometry.Geometry.to_wkt], [`from_wkt`][gometry.from_wkt],
and [`Geometry.force_2d`][gometry.Geometry.force_2d].

```python exec="on" source="block" result="text"
import gometry as gm

wire = gm.Point(1, 2).to_wkt()
parsed = gm.from_wkt(wire)
projected = gm.Point(-73.9, 40.7, crs=4326).to_crs(3857)
flattened = gm.Point(1, 2, z=3).force_2d()
print(parsed.to_wkt(), projected.crs.to_epsg(), flattened.to_wkt())

```

### 3. Use `_all` for pooled many-to-one operations

An `<op>_all` function pools many input geometries into one aggregate result;
row-wise array methods keep results associated with their source rows. This
distinction makes aggregation explicit. See [`union_all`][gometry.union_all],
[`polygonize_all`][gometry.polygonize_all], and
[`coverage_union_all`][gometry.coverage_union_all].

```python exec="on" source="block" result="text"
import gometry as gm

points = [gm.Point(0, 0), gm.Point(1, 1)]
print(gm.union_all(points).to_wkt())

```

### 4. Return `Groups` when each input row can produce a different number of results

Ragged array operations return one [`Groups`][gometry.Groups] entry per source
row, preserving row association; a missing input row has an empty group. Callers
can use each result without guessing which input produced it. See
[`split`][gometry.split] and the [`Groups` reference][gometry.Groups].

```python exec="on" source="block" result="text"
import gometry as gm

line = gm.LineString([(0, 0), (2, 0)])
groups = gm.split(gm.GeometryArray([line, None, line]), gm.Point(1, 0))
print(type(groups).__name__, groups.offsets.tolist(), groups[1].to_wkt())

```

The cutter lies exactly on these CRS-free lines, and the default split
tolerance is zero.

### 5. Select an operation's representation or added output with a typed keyword

A representation, output, or algorithm choice that belongs to one operation is
a keyword option on that operation, not a second operation name. Literal
overloads let type checkers narrow results for explicit choices. This keeps
related behavior discoverable together. See
[`CRS.to_projjson`][gometry.CRS.to_projjson],
[`Geometry.voronoi`][gometry.Geometry.voronoi],
[`distance`][gometry.distance], and
[`polygonize_all`][gometry.polygonize_all].

```python exec="on" source="block" result="text"
import gometry as gm

mapping = gm.CRS(4326).to_projjson(as_dict=True)
print(type(mapping).__name__, mapping["type"])

```

### 6. Keep supported input forms under one operation name

When an operation accepts scalar and batch inputs, overloads describe those
forms under the same name and preserve the corresponding result shape. Missing
values admitted in a batch remain `None` rows. Callers can use the form that
matches their data without converting it to a separate API. See
[`from_wkt`][gometry.from_wkt] and the
[array result contract](../guide/arrays.md).

```python exec="on" source="block" result="text"
import gometry as gm

scalar = gm.from_wkt("POINT (1 2)")
batch = gm.from_wkt(["POINT (1 2)", None])
print(type(scalar).__name__, type(batch).__name__, batch.to_wkt())

```

### 7. Preserve known facts in public types

Annotations retain geometry kind, nullability, and scalar or container result
shape when the inputs establish them; they do not promise inputs that runtime
admission rejects. Precise types avoid unnecessary checks and make invalid
calls visible to type checkers. See [`GeometryArray`][gometry.GeometryArray]
and its [`buffer` method][gometry.GeometryArray.buffer].

```python exec="on" source="block" result="text"
import gometry as gm

point = gm.Point(1, 2)
single: gm.Polygon = point.buffer(1)
dense: gm.GeometryArray[gm.Polygon] = gm.GeometryArray([point]).buffer(1)
nullable_input: gm.GeometryArray[gm.Point | None] = gm.GeometryArray([point, None])
nullable: gm.GeometryArray[gm.Polygon | None] = nullable_input.buffer(1)
print(type(single).__name__, type(dense).__name__, nullable.is_missing.tolist())

```

### 8. Prefer one obvious way, with exceptions for distinct capabilities

A second spelling is justified when it adds a distinct capability or is the
natural idiom in its context, not when it merely renames the same operation.
For example, [`SpatialIndex.candidates`][gometry.SpatialIndex.candidates]
returns a bounding-box prefilter, while
[`SpatialIndex.query`][gometry.SpatialIndex.query] refines candidates with an
exact predicate. Keeping them separate makes that difference visible.

```python exec="on" source="block" result="text"
import gometry as gm

index = gm.SpatialIndex([gm.Polygon([(0, 0), (2, 0), (0, 2)])])
point = gm.Point(1.8, 1.8)
print(index.candidates(point).tolist(), index.query(point).tolist())

```

The CRS-free point lies inside the polygon's bounds but outside the triangle.

### 9. Treat Python and ecosystem protocols as public API

Python dunders, standard-library protocols, and NumPy, pandas, Arrow, and Polars
integrations are designed, typed, and tested as part of the public surface.
Protocol support lets geometry values compose with ordinary Python and
ecosystem tools. See [`GeometryArray`][gometry.GeometryArray],
[NumPy arrays & coordinates](../ecosystem/numpy.md), and
[Ecosystem & interoperability](../ecosystem/index.md).

```python exec="on" source="block" result="text"
import numpy as np
import gometry as gm

array = gm.GeometryArray([gm.Point(1, 2), gm.Point(3, 4)])
print(len(array), array[0].to_wkt(), np.asarray(array, dtype=object).dtype)

```
