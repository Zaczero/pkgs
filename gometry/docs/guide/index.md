---
description: "A task-oriented map of gometry's user guide: construct, measure, query, aggregate, repair, exchange, and choose operations."
---

# User guide

Choose a task below, or look up a function in the [API reference](../api/index.md).

## Construct and inspect

- [Geometry & dimensions](geometry.md) — construct the seven geometry families,
  inspect coordinates, and work with XY/XYZ/XYM/XYZM values.

## Measure and transform

- [CRS, units & measurement](crs.md) — declare or transform a frame, choose the
  metric, and handle antimeridian and epoch rules.

## Test relationships and build outputs

- [Spatial predicates](predicates.md) — choose boundary-aware relationships such
  as `contains`, `covers`, and `within`.
- [Constructive operations](constructive.md) — buffer, overlay, simplify, split,
  and derive new geometry.

## Index, join, or aggregate on a grid

- [Spatial indexing & joins](indexing.md) — retrieve bbox candidates, refine with
  an exact predicate, find nearest features, or make many-to-many joins.
- [Grids & geocodes](grids.md) — assign H3/S2/geohash/tile keys, select a
  `cell_rule`, aggregate by cell, then refine against source geometry.

## Validate and repair

- [Validation & repair](validation.md) — inspect invalid geometry, repair it,
  and choose a precision or cleanup operation.

## Process batches and exchange data

- [Arrays & performance](arrays.md) — use packed arrays, pairwise broadcasting,
  ragged results, and bulk ingestion.
- [Ecosystem & interoperability](../ecosystem/index.md) — choose NumPy, Arrow,
  dataframe, or text/binary interchange boundaries.

## Choose between similar operations

- [Choosing the right tool](choosing.md) — compare predicates, prepare versus
  index versus join, clipping, vertex cleanup, and polygonal coverages.
