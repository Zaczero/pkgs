---
description: Current release benchmark results for gometry and how to interpret their scope.
---

# Benchmarks

--8<-- "benches/results/current/summary.md"

Published tables link to raw measurements, correctness checks, and build
identities. Results apply to the recorded workload, versions, and host;
they are not a universal speed comparison.

## What the run measures

The matrix pairs each gometry operation against its ecosystem counterpart:
53 paired logical operations across array construction and I/O, geometry, CRS
and geodesy, discrete global grids, and spatial indexes, plus six three-point
scaling curves over index cardinality, query selectivity, candidate
amplification, n-ary overlay input count, grid output count, and the
prepared-predicate batch boundary.

Each pair runs in six independent blocks — three leading with gometry, three
leading with the comparator — each block a fresh process that executes both
arms after two untimed prewarm calls. Analysis bootstraps the paired median
ratio over those blocks and classifies it against a declared 3% practical
equivalence margin: `faster` and `slower` require the whole interval to clear
the margin, `similar` requires it to sit inside, and anything else is
`inconclusive`. Every row carries an untimed fail-closed oracle that checks row
counts, missingness, types, axes, coordinates, and topology, so a wrong answer
cannot be published as a fast one.

Setup and artifact verification are outside the operation timer. Optional
hardware counters measure the calling thread's CPU work, not latency.
Unavailable measurements remain marked as unavailable. Host records include
CPU, governor, cache topology, and memory policy where the kernel exposes them.

## See Also

- [Arrays & performance](../guide/arrays.md) — packed construction and batch results.
- [Internals](internals.md) — durable ownership and interchange boundaries.
- [Compatibility](compatibility.md) — supported runtime and integration matrix.
