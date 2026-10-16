---
description: Current release benchmark results for gometry and how to interpret their scope.
---

# Benchmarks

## Scaling

These curves show median call times across six paired process blocks on the
Ryzen 9 5950X; whiskers span the observed block medians, not confidence intervals.
Both implementations receive the same inputs and produce equivalent
results. The tables below include all measured workloads, including slower and
inconclusive comparisons.

![Spatial index query times for gometry and Shapely as indexed polygon count increases.](../assets/benchmark-spatial-index-cardinality-light.svg#only-light)
![Spatial index query times for gometry and Shapely as indexed polygon count increases.](../assets/benchmark-spatial-index-cardinality-dark.svg#only-dark)

The spatial-index workload runs 1,000 bounding-box candidate queries per call;
it does not include exact predicate refinement.

![Union call times for gometry and Shapely on increasing counts of disjoint polygons.](../assets/benchmark-overlay-disjoint-input-count-light.svg#only-light)
![Union call times for gometry and Shapely on increasing counts of disjoint polygons.](../assets/benchmark-overlay-disjoint-input-count-dark.svg#only-dark)

The union workload combines disjoint unit squares. It describes that input
family, not overlapping polygons or every overlay operation.

## All measured comparisons

Median time per call; lower is better. Shading marks the faster implementation.
Relative results describe gometry.

<details markdown="1" class="benchmark-results">
<summary>60 comparisons by API and workload</summary>

--8<-- "benches/results/current/summary.md"

</details>

## What the run measures

The matrix pairs 36 individual workloads against ecosystem counterparts across
array construction and I/O, geometry, CRS and geodesy, discrete global grids,
and spatial indexes, plus eight three-point
scaling curves over index cardinality, query selectivity, candidate
amplification, n-ary and disjoint overlay input counts, polygon hole count,
grid output count, and the
prepared-predicate batch boundary.

Each pair runs in six independent blocks — three leading with gometry, three
leading with the comparator — each block a fresh process that executes both
arms after two untimed prewarm calls. Analysis bootstraps the paired median
ratio over those blocks and classifies it against a declared 3% practical
equivalence margin: `faster` and `slower` require the whole interval to clear
the margin, `similar` requires it to sit inside, and anything else is
`inconclusive`. Untimed correctness checks compare row counts, missingness,
types, axes, coordinates, and topology.

Setup and artifact verification are outside the operation timer. Optional
hardware counters measure the calling thread's CPU work, not latency.
Unavailable measurements remain marked as unavailable. Host records include
CPU, governor, cache topology, and memory policy where the kernel exposes them.

## See Also

- [Arrays & performance](../guide/arrays.md) — packed construction and batch results.
- [Internals](internals.md) — durable ownership and interchange boundaries.
- [Compatibility](compatibility.md) — supported runtime and integration matrix.
