---
description: "A task-oriented map of gometry's user guide: construct, measure, query, aggregate, repair, exchange, and choose operations."
---

# User guide

For signatures and return types, use the [API reference](../api/index.md).

## Create, inspect, and measure

<span id="construct-and-inspect"></span>
<span id="measure-and-transform"></span>

- [Geometry & dimensions](geometry.md) — construct the seven geometry families,
  inspect coordinates, and work with XY/XYZ/XYM/XYZM values.

- [CRS, units & measurement](crs.md) — declare or transform a frame, choose the
  metric, and handle antimeridian and epoch rules.

## Query and transform

<span id="test-relationships-and-build-outputs"></span>
<span id="index-join-or-aggregate-on-a-grid"></span>
<span id="choose-between-similar-operations"></span>

- [Spatial predicates](predicates.md) — choose boundary-aware relationships such
  as `contains`, `covers`, and `within`.
- [Constructive operations](constructive.md) — buffer, overlay, simplify, split,
  and derive new geometry.

- [Spatial indexing & joins](indexing.md) — retrieve bbox candidates, refine with
  an exact predicate, find nearest features, or make many-to-many joins.
- [Grids & geocodes](grids.md) — assign H3/S2/geohash/tile keys, select a
  `cell_rule`, aggregate by cell, then refine against source geometry.
- [Choosing the right tool](choosing.md) — compare predicates, prepare versus
  index versus join, clipping, vertex cleanup, and polygonal coverages.

## Work with datasets

<span id="validate-and-repair"></span>
<span id="process-batches-and-exchange-data"></span>

- [Validation & repair](validation.md) — inspect invalid geometry, repair it,
  and choose a precision or cleanup operation.

- [Arrays & performance](arrays.md) — use packed arrays, pairwise broadcasting,
  ragged results, and bulk ingestion.
- [Features & attributes](features.md) — keep geometries, properties, and IDs
  aligned while selecting and updating rows.
- [Ecosystem & interoperability](../ecosystem/index.md) — choose NumPy, Arrow,
  dataframe, or text/binary interchange boundaries.
