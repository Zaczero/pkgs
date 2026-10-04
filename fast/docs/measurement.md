# Measurement and inference

This reference describes `fast`'s invocation boundary, resource scopes, effort
planning, and timing inference for Linux command comparisons.

## Invocation boundary

The single-threaded collector prepares arguments, environment, input descriptors,
and capture buffers before timing. It then timestamps process creation, attaches
available performance events before releasing the child to exec, and waits for
the root command's completion. Timing ends before residual stderr draining.

The child has its own process group. A subreaper retains ownership of orphaned
descendants without creating cgroups or changing inherited scheduling policy.
Interruption signals owned work, escalates if necessary, and reports the
completed runs. This is lifecycle tracking, not containment: an uninterruptible kernel
task cannot be made immediately reapable by userspace.

Stderr capture keeps the first and last 32 KiB while continuing to drain the pipe.
Each draining turn is bounded so continuous output cannot starve completion or
timeouts. Post-completion draining has a 100 ms limit.

## Resource scopes

`wait4` supplies user/system CPU, faults, switches, and maximum RSS.
Linux's [`getrusage` documentation](https://man7.org/linux/man-pages/man2/getrusage.2.html)
defines the high-water and waited-child semantics. Sampled process or thread CPU
is never added to wait-accounted CPU.

Wait-accounted RSS preserves memory inherited across `fork` and `exec`. The child
reports its launch-time RSS high-water mark over the existing release channel,
before command execution. The default report shows mean peak and mean launch
values together. This exposes a parent-memory floor without subtracting one
high-water mark from another: such a subtraction cannot recover workload memory.
It adds no extra command execution, memory scan, or helper process.

Hardware instructions, cycles, and branch misses use separate ungrouped
events with inheritance, user-space-only privilege scope, and enable-on-exec.
The collector attaches fresh events to each child before releasing it to exec.
Each observation retains raw,
enabled-time, and running-time readings. The report uses scaled counts only when
at least 95% of enabled time was scheduled. These are supplemental descriptive
measurements. The [`perf_event_open` interface](https://man7.org/linux/man-pages/man2/perf_event_open.2.html)
documents the inheritance/group-read restrictions this arrangement avoids.

On AMD Family 19h Model 21h (Zen 3 Vermeer), the standard policy also collects
core-to-L2 request misses: raw event `0x64`, unit mask `0x09`. These exclude
L2 prefetch requests. The report labels the cache level
explicitly and normalizes misses by retired instructions; it does not interpret
the count as stall time or a diagnosis of a memory bottleneck. Other processors
retain the generic counters without guessing an equivalent raw event.

Instructions per cycle and branch-miss ratios require full counter scheduling
through every selected invocation. Ratios use summed counts, not an average of
per-run ratios. Instruction variation is the sample standard deviation divided
by the mean; it describes repeatability, not confidence in a timing improvement.
Peak and launch RSS are means of the individual invocation high-water marks.

Task sampling follows the root and discovered child lists, not a scan of every
process on the host. Identity includes invocation, PID namespace, task ID, and
kernel start ticks. The initial cadence is 100 ms, increasing to at least 200
times the preceding sweep's collector CPU cost. Large sweeps yield between
16-task chunks for at least 200 times each chunk's collector CPU cost, so the
initial sweep cannot run as an uninterrupted burst. A sweep still in progress
when the command exits marks task detail incomplete. At most 1,024 task identities and
bounded activity samples are retained per invocation. Task detail survives only
for the first complete measured invocation of each command.

These are sampled accounting changes, not scheduler events or function profiles.
The [`proc` documentation](https://docs.kernel.org/filesystems/proc.html) describes
the underlying process/thread records and their limitations.

## Planning

`--runs N` measures exactly N runs, after any `--warmup` runs. Otherwise a pilot
measures each run's cost, including hooks and collection, and its run-to-run
variation. The pilot ends after eight runs, or earlier when another run would
carry the elapsed time past 20% of the target. Later pilot runs predict the cost
of the rest better than the first, so the cost estimate is their mean; with a
single pilot run it is that run.

The first run pays for cold page caches, lazy loading, and frequency ramp-up.
Without explicit `--warmup`, it is discarded when the median of the later pilot
runs costs at most 5% of the target, so the discarded run is cheap. A command
whose first run took five times its usual time would otherwise bias a 100-run
mean by 4%. Slow commands keep the first run: discarding it would cost a large
share of the few runs they can afford.

The kept pilot runs count toward the result. Production adds the runs that fit in
the remaining target, up to the larger of a precision estimate and 100. The
precision estimate is the run count at which every pair's interval would reach
about ±1%, assuming independent runs with the pilot's variation. When the budget
allows, 100 runs give the batch means below enough length to absorb correlation
between consecutive runs. The default target is a goal: slow commands still get
at least 3 runs, the fewest that usually bound an interval. An explicit `--time`
is a strict budget, so the plan may stop after the first run.

Planning uses run costs and the pilot's variation, never a comparison's outcome.
Choosing the run count from the variance is an internal pilot design, and the
automatic calibration cells below include this selection step; fixed-count
coverage alone does not validate the planner. An interrupted experiment analyzes
its completed runs. No estimates are shown while runs continue, but stopping
because of an externally observed result can still bias the comparison. A failed
run can depend on the measured behavior, so a failure leaves the completed runs
descriptive.

## Arithmetic means and simultaneous intervals

For each command pair, the estimand is the ratio of arithmetic mean elapsed
times; a single command's estimand is its mean. The method,
`fieller-batch-means-bonferroni-v4`, splits the chronological runs into
contiguous batches whose sizes differ by at most one run: one batch per run up to
ten runs, ten batches up to 100 runs, then the square root of the run count,
capped at 30. Every command's runs share the same batches, so each batch pairs
the commands under the same conditions.

The estimated covariance of two commands' overall means is the size-weighted
spread of their batch means, `sum m_j (x_j - X)(y_j - Y) / ((b - 1) n)`. For
independent, normally distributed runs this is exact with `b - 1` degrees of
freedom, also for unequal batch sizes. Batches longer than the correlation
between consecutive runs restore that independence approximately; with one run
per batch the method is the classical paired analysis.

Fieller's theorem turns these estimates into a confidence set for the ratio of
means: the values `R` with `(Y - R X)^2 <= t^2 Var(Y - R X)`. When the
denominator's mean is not distinguishable from zero at this confidence, the set is
unbounded, and the comparison is unresolved without an interval. The t quantile
has `b - 1` degrees of freedom at level `1 - 0.05 / (2m)` for the `m` command pairs
(Bonferroni), so all intervals hold together with at least 95% confidence.
Elapsed times are positive, so a set reaching below zero is cut at zero, which
cannot exclude the true ratio. A single command uses the t interval for its mean,
likewise cut at zero.

The interval treats the batch means as independent. A one-way analysis-of-variance
test checks that on each comparison's per-run residuals `y - R x`: drift, load
changes, and runs that influence their successors make batch means differ more
than within-batch variation explains. A comparison whose F statistic exceeds its
`1 - 0.01 / m` quantile, with at least two runs per batch, is reported as
correlated; its interval may be too narrow.

Identical runs, or runs whose ratio never varies, have no measurable variation;
they produce no interval rather than a zero-width certificate. Launch repetitions
describe this binary, input, and environment; they do not measure variation
across builds or machines.

## Calibration evidence and limits

The source-owned `calibration` Cargo benchmark simulates chronological runs in
randomized order and runs the production analyzer, optionally behind the
production planner. It reports joint coverage, false directions, detection,
experiments without any claim, correlation flags, interval width and bias, with
one-sided 95% Wilson binomial bounds. Each cell below contains 10,000 experiments
on the held-out seed `d91453c274b8109a`, which was not used while choosing the
method. Simulated runs take about 1 ms each.

| Model | Runs | Joint coverage | False direction | No claim | Detects the faster command |
| --- | ---: | ---: | ---: | ---: | ---: |
| Four independent lognormal commands, log SD 0.1 | 3 | 95.23% | 4.77% | 21.98% | — |
| Four independent lognormal commands, log SD 0.1 | 10 | 95.43% | 4.57% | 0.00% | — |
| Four independent lognormal commands, log SD 0.1 | 100 | 95.37% | 4.63% | 0.00% | — |
| Four independent lognormal commands, log SD 0.1 | 1000 | 95.69% | 4.31% | 0.00% | — |
| Two quiet commands, log SD 0.02; B takes 5% less time | 2 | 94.58% | 0.00% | 0.00% | 16.52% |
| Two quiet commands, log SD 0.02; B takes 5% less time | 3 | 94.65% | 0.00% | 0.00% | 41.76% |
| Two quiet commands, log SD 0.02; B takes 5% less time | 5 | 94.95% | 0.00% | 0.00% | 85.39% |
| Two quiet commands, log SD 0.02; B takes 5% less time | 10 | 95.18% | 0.00% | 0.00% | 99.92% |
| Two quiet commands with equal means | 3 | 94.65% | 5.35% | 0.00% | — |
| Two quiet commands with equal means | 100 | 94.94% | 5.06% | 0.00% | — |
| B takes 10% less time, log SD 0.1 | 10 | 95.43% | 2.40% | 0.00% | 23.16% |
| B takes 10% less time, log SD 0.1 | 37 | 95.59% | 2.37% | 0.00% | 83.79% |
| B takes 10% less time, log SD 0.1 | 100 | 95.37% | 2.54% | 0.00% | 99.95% |
| Unequal log SDs 0.03, 0.06, 0.09, 0.12 | 100 | 95.95% | 4.05% | 0.00% | — |
| Shared correlated multiplicative noise | 100 | 95.65% | 4.35% | 0.00% | — |
| Lognormal skew, log SD 0.7 | 10 | 97.69% | 2.31% | 30.18% | — |
| Lognormal skew, log SD 0.7 | 100 | 96.13% | 3.87% | 0.00% | — |
| A run after A takes 5% longer | 100 | 95.77% | 2.33% | 0.00% | — |
| Eight independent commands | 5 | 95.12% | 4.88% | 0.00% | — |
| Eight independent commands | 100 | 95.26% | 4.74% | 0.00% | — |
| Single-command arithmetic mean | 100 | 95.26% | 0.00% | 0.00% | — |
| Automatic plan, target worth 2 runs | 3.0 mean | 95.23% | 4.77% | 21.98% | — |
| Automatic plan, target worth 200 runs | 198.5 mean | 95.77% | 4.23% | 0.00% | — |
| Automatic plan, quiet pair, 5% faster | 100.4 mean | 95.00% | 0.00% | 0.00% | 100.00% |
| Automatic plan, first run of A five times slower | 197.5 mean | 95.70% | 4.30% | 0.00% | — |

Joint coverage counts an experiment when every bounded interval contains its true
ratio; an unbounded set makes no claim and so cannot be wrong. The no-claim
column shows how often that happened, so abstention is not mistaken for evidence.
False direction counts experiments with at least one directional verdict against
the truth; with equal means, every directional verdict is false. Across all 118
supported cells, from two runs to 1,000, the one-sided 95% lower coverage bound
is at least 94.15% and the upper false-direction bound at most 5.80%. Automatic
cells include the pilot, the warmup decision, and the variance-dependent run count.

The limitations matter just as much:

- Command-specific AR(1) correlation 0.5 between consecutive runs covered 62.59%
  at 10 runs and 88.41% at 37, then 93.85% at 100 and 95.27% at 1,000. Below
  20 runs the correlation check cannot run; it flagged 25.16% of experiments at
  20 runs, 87.17% at 100 and nearly all at 1,000, where batching already
  restores coverage.
- Correlation 0.9 covered 67.18% at 100 runs and 89.78% at 1,000, flagged in
  every experiment. Strongly persistent conditions are outside the validated model.
- A one-in-1,000, 100-times-slower event affecting only one command covered 9.72%
  at 100 runs. No interval can infer a tail that the runs usually never encounter.
- Linear drift, periodic load, and carryover simulations do not establish
  stationarity. Their coverage concerns their explicit finite-run mean. Drift was
  flagged in 75.04% of experiments at 100 runs.
- With independent runs, the correlation check flagged about 1% of experiments,
  its designed false-alarm rate.
- A five-times-slower first run in a fixed 100-run experiment mostly widened the
  interval (97.43% coverage) rather than biasing it past its width; the automatic
  warmup discards that run when it is cheap.

A stationary bootstrap with conservative corrections, the previous method, needed
100 runs before any conclusion and covered 98.6–99.9% where Fieller batch means
cover close to 95%. At 100 runs of four independent commands its intervals were
about 45% wider (mean log width 0.136 versus 0.093).

Low variation does not establish independence. Additional automatic-plan cases
use two quiet commands (log SD 0.02), a target worth 200 runs, and 50,000
experiments on the separate seed `b7809f3a41c26de5`:

| Model | Mean runs | Coverage | False direction | Correlation flagged |
| --- | ---: | ---: | ---: | ---: |
| Independent, equal means | 100.43 | 95.05% | 4.95% | 1.15% |
| Independent, B takes 5% less time | 100.43 | 95.05% | 0.00% | 1.15% |
| Command-specific correlation 0.5, equal means | 100.24 | 93.62% | 6.38% | 58.30% |
| Command-specific correlation 0.9, equal means | 100.00 | 78.19% | 21.81% | 99.81% |

The independent effect case detected the faster command in every experiment.
The correlated cases show why a quiet pilot alone cannot justify a short plan:
even the preferred 100 runs do not restore nominal coverage under persistent
noise. A correlation flag calls the interval into question; it does not correct it.

Reproduce a cell from the repository root:

```sh
cargo bench -p fast --bench calibration -- 10000 100 ar 4 d91453c274b8109a
cargo bench -p fast --bench calibration -- 10000 200 cold 4 d91453c274b8109a --auto
```

Arguments are experiment count, runs (with `--auto`, the planning target in runs'
worth of time), model, worker count, and hexadecimal seed. Run `--help` for model
names. Simulations are development tools; the installed `fast` binary has no
simulation mode.

All cells are retained in [`calibration.csv`](../benches/calibration.csv); the
deterministic model source is [`calibration.rs`](../benches/calibration.rs). The
simulations draw from `fast`'s xoshiro256++ generator, so the seed reproduces
these cells exactly. Coverage is a result for these models, not a
distribution-free guarantee.

## Observer perturbation

The `observer` example uses the production runner with paired randomized
observer-on/off invocations. The off arm retains command launch, wait accounting,
and diagnostic draining, but omits PMU attachment and task sampling. This
distinguishes observation cost from ordinary process-launch cost.

The four-event Vermeer policy and paced task sampler were measured on an AMD
Ryzen 9 5950X, Linux 7.2.8, nightly Rust `0abfedbc7`, release profile,
with inherited scheduling and background load:

| Workload | Runs | On/off time ratio | 95% interval (v3 bootstrap) |
| --- | ---: | ---: | --- |
| Native launch fixture | 1,000 | 1.1265 | 1.0334–1.2279 |
| 1 ms sleep | 100 | 1.0289 | 0.7866–1.3458 |
| 10 ms sleep | 100 | 1.0713 | 0.9544–1.2024 |
| 100 ms sleep | 100 | 1.0153 | 0.9912–1.0400 |
| Ten million integer recurrence steps | 100 | 0.9583 | 0.7440–1.2344 |
| Eight threads, ten million steps each | 100 | 1.0408 | 0.7292–1.4856 |
| 1 MiB stderr | 100 | 0.9480 | 0.6336–1.4185 |
| Eight threads sleeping 500 ms | 100 | 1.0066 | 0.9986–1.0147 |
| 1,100 threads sleeping 200 ms | 100 | 1.0374 | 1.0290–1.0459 |

The launch fixture averaged 13.324 ms without observation and 15.009 ms with
observation on this loaded host. The 1,100-thread fixture paid about 9.0 ms more
with observation enabled. These are observed costs in this environment, not
fixed overhead constants. The remaining intervals include parity; several are
wide, and none establishes zero overhead. Short-command results include launch
and measurement cost, which is not subtracted.

All instruction and cycle readings in this matrix were nonzero and available.
The 1,100-thread runs each retained 16 task identities before command completion,
with the unfinished sweep marked incomplete. The median adaptive full-sweep
interval was 596 ms. This deliberately exercises bounded collection rather than
claiming complete visibility into all workers.

Reproduce the launch comparison:

```sh
cargo build -p fast --release --examples
target/release/examples/observer --runs 1000 -- target/release/examples/workload
```

For other rows, pass `--sleep-ms 1`, `--iterations 10000000`, `--threads 8`,
or `--stderr-bytes 1048576` to the workload as appropriate. The observer emits
all chronological observations and its analysis as JSON on stdout.

## Optimization checks

A public `fast` comparison measured paced and unpaced collectors in 100
randomized runs, each running the 1,100-thread, 200 ms fixture once and emitting
JSON. Builds used the same compiler, source path, and target directory. The
paced/unpaced elapsed ratio was 0.9955, with interval 0.9898–1.0012: an elapsed
improvement was not established. Mean wait-accounted CPU time fell from
166.407 ms to 144.449 ms, an observed 13.2% reduction. Pacing is retained for
that reduction in work and its smaller initial scanning burst, at the cost of
less task detail for short-lived commands with many workers. Nested collectors
multiplexed the PMU events, so their instruction and cycle counts were
ineligible for comparison.

A matched release-build comparison used `fast` itself to measure 100 randomized
runs of nested ten-run JSON experiments. Adding a 64 KiB JSON output buffer
gave a buffered/unbuffered elapsed ratio of 1.0031, with interval
0.9717–1.0355. The standard stdout writer remains: the extra buffer did not
establish a useful improvement.

Persistent disabled collector-side PMU events were also evaluated as an
alternative to per-child attachment. On Linux 7.2.8, a 2,000-invocation
repeated-fork check stopped receiving instruction and cycle counts after
invocation 235 and produced 3,528 zero readings. Fresh per-child attachment
produced no zero readings for the same commands. The collector therefore gives
each invocation fresh events rather than retaining cross-invocation counter
state.
