---
description: Whether a gometry release benchmark run is published, and how one is produced.
---

# Benchmarks

--8<-- "benches/results/current/summary.md"

This page reports one accepted full release run or nothing at all. It does not
combine runs, publish smoke or filtered measurements, or repeat a timing claim
whose evidence is absent. Numbers you find elsewhere — an issue, a commit
message, a comparison someone ran on their laptop — are not release evidence
and are not reproduced here.

## Producing a run

```bash
uv run python benches/drivers/bench.py --profile release
uv run python benches/drivers/bench.py --analyze target/bench/results/<run-id>/run.json
```

`<run-id>` is the directory the measurement command prints. Measurement writes
only raw artifacts and a run manifest; it cannot touch tracked evidence.
Analysis reads exactly one manifest and produces the normalized review files
that a publication consumes.

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

Runs record the exact interpreter, extension digest, build profile, dependency
lock, and native-library identity for both implementations, along with the
host's CPU, governor, cache topology, and memory policy. A host fact the
kernel does not expose stays recorded as unavailable rather than becoming a
claim about some other machine.

## See Also

- [Arrays & performance](../guide/arrays.md) — packed construction and batch results.
- [Internals](internals.md) — durable ownership and interchange boundaries.
- [Compatibility](compatibility.md) — supported runtime and integration matrix.
