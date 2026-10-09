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

```bash title="template: replace <run> with the directory printed by the first command"
uv run python benches/drivers/bench.py --profile release --gometry-receipt <release-or-runner-receipt>
uv run python benches/drivers/bench.py --publish target/bench/results/<run>/run.json
```

Measurement writes raw artifacts and a run manifest under `target/` and never
touches tracked files. Smoke runs default to this worktree's
`target/quality-runtime/receipt.json`; release runs must select an existing
release manifest or runner receipt. The driver never prepares a missing runtime.
DEV quality receipts are diagnostic subjects and cannot publish release results.
Publication validates that one run — complete, release profile, oracle passed,
every block measured the recorded build — and rewrites
`benches/results/current/summary.md`, the table above. Git versions the table;
the raw evidence stays with the run.

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

Each measured process admits its selected receipt before importing Gometry.
The driver checks the process's ready record before allowing its first timed
operation; an import-path fallback, a different worktree facade, or an extension
whose bytes do not match the receipt's wheel member is rejected. The process
identity binds its interpreter and ABI, build profile, Python package members
and distribution owner, native extension, wheel archive, and receipt digest.
A null producer commit leaves source attribution unknown but still requires
those exact byte bindings. Per-invocation process IDs identify the observation;
they are not treated as stable producer identity.

The operation timers begin after admission and setup. `bench_ab.py --perf`
retains its whole-command counter scope, including startup and admission; those
counters are diagnostic and do not qualify as release latency. Runs also record
the host's CPU, governor, cache topology, and memory policy. A host fact the
kernel does not expose stays recorded as unavailable rather than becoming a
claim about some other machine.

## See Also

- [Arrays & performance](../guide/arrays.md) — packed construction and batch results.
- [Internals](internals.md) — durable ownership and interchange boundaries.
- [Compatibility](compatibility.md) — supported runtime and integration matrix.
