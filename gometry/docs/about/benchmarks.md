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

<span id="scaling-curves"></span>

Whiskers span the six paired-block medians.

<!-- benchmark-scaling -->

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
