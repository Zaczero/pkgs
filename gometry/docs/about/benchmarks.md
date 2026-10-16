---
description: Current release benchmark results for gometry and how to interpret their scope.
hide:
  - navigation
---

# Benchmarks

## API comparisons {#all-measured-comparisons}

Median call times.

--8<-- "benches/results/current/summary.md:comparisons"

## Scaling

Whiskers span the six paired-block medians.

![Spatial index query times for gometry and Shapely as indexed polygon count increases.](../assets/benchmark-spatial-index-cardinality-light.svg#only-light)
![Spatial index query times for gometry and Shapely as indexed polygon count increases.](../assets/benchmark-spatial-index-cardinality-dark.svg#only-dark)

1,000 bounding-box candidate queries per call, without exact predicate refinement.

![Union call times for gometry and Shapely on increasing counts of disjoint polygons.](../assets/benchmark-overlay-disjoint-input-count-light.svg#only-light)
![Union call times for gometry and Shapely on increasing counts of disjoint polygons.](../assets/benchmark-overlay-disjoint-input-count-dark.svg#only-dark)

Union of disjoint unit squares.

--8<-- "benches/results/current/summary.md:scaling"

## What the run measures

The run covers 36 individual workloads and eight three-point scaling studies.

Each pair runs in six independent blocks — three leading with gometry, three
leading with the comparator — each block a fresh process that executes both
arms after two untimed warm-up calls. Analysis bootstraps a 95% interval for the
paired median ratio and classifies it against a 3% practical
equivalence margin: `faster` and `slower` require the whole interval to clear
the margin, `similar` requires it to sit inside, and anything else is
`inconclusive`. Untimed correctness checks compare row counts, missingness,
types, axes, coordinates, and topology.

Fixture setup is outside the operation timer.

--8<-- "benches/results/current/summary.md:environment"

--8<-- "benches/results/current/summary.md:evidence"

## See Also

- [Arrays & performance](../guide/arrays.md) — packed construction and batch results.
- [Internals](internals.md) — durable ownership and interchange boundaries.
- [Compatibility](compatibility.md) — supported runtime and integration matrix.
