# Bench harness

Contributor notes for measuring h2corn. The public
[Benchmarks](../docs/benchmarks.md) page shows cross-server plots from a
single observed run; use the tools here when deciding whether a *code
change* improves performance.

| Tool | Purpose |
| ---- | ------- |
| `bench.py` | Repeated cross-server scenarios → raw JSON and SVGs |
| `compare.py` | Paired control vs candidate on one URL/workload |
| `matrix.py` | Expands `compare.py` across `matrix.toml` scenarios |
| `mask_kernel_compare.py` | WebSocket mask kernel microcomparison |

Unique raw runs under `results/runs/` are gitignored. Canonical SVGs are
checked in; `bench.py` replaces them only with an explicit successful
`--publish` run.

## Cross-server publication (`bench.py`)

The default command stages every raw record and SVG under a unique run
directory, so smoke tests and filtered runs cannot overwrite the canonical
plots. Publication requires the complete scenario/server matrix and the exact
canonical configuration: eight trials containing complete forward/reverse
Latin rotations, one-second request-path warmup, ten-second measurement, and
disjoint server/load CPU sets:

```bash
uv run python bench/bench.py \
  --server-cpus 2-5 --load-cpus 8-15,24-31 \
  --management-cpus 0 \
  --load-worker-threads 16 --trials 8 --publish
```

The harness snapshots the actual server and load-generator executables,
imported h2corn package/native extension, benchmark application, k6 script,
commands, source identity, selected-CPU topology and LLC domains, online/process
affinity, governor/driver/EPP, boost, kernel command line, and transparent
huge-page state. Every benchmark-driver thread is pinned to the separate
management CPU 0; this includes orchestration, 20 Hz resource sampling, and the
CPU interference monitor. The harness records all thread-affinity masks and
freezes the full snapshot once at run start. Before every server launch it
re-checks a cheap stat fingerprint (path, size, mtime, inode) of the same
hashed file set, re-runs the full hash/git/system verification whenever that
fingerprint changes, and always re-verifies fully after the suite. These are
pinned CPU roles on a noisy host, not kernel-isolated CPUs. The ordinary
one-second request-path warmup is not quiet-gated. Immediately afterward, before
each measured trial, and before every headroom generator run (including its two
warmups), a one-second `/proc/stat` probe covers physical cores, SMT siblings,
and complete LLC domains. Aggregate utilization must remain at or below 10% and
every logical CPU at or below 15%. During each measured trial and headroom run,
unused server siblings/LLC CPUs are checked over fixed one-second exposure and
the complete-load aggregate with the same limits. A subsecond final sample is
retained raw and normalized to one second for the gate; a newly busy host aborts
rather than producing a silently degraded publication. Every measured run
requires all configured workers to answer, runs one second of unmeasured
request-path load on the same server process before measurement, and verifies
the exact HTTP body or WebSocket echo before warmup and after measurement.
Warmup commands, raw output, and resource usage are retained alongside all
throughput/latency samples through p99.9 and their ranges. Before publication,
every scenario's fastest server also gets two complete load-generator warmups
and seven measured R/F/F/R blocks: twenty-eight measured runs and fourteen
adjacent paired gains comparing twelve versus sixteen generator workers on the
identical CPU set. oha receives `TOKIO_WORKER_THREADS`; k6 receives
`GOMAXPROCS`. A load-side bottleneck is ruled out only when an exact one-sided
sign test proves the paired median gain is strictly below 2%. Its threshold is
Bonferroni-corrected across the 21-scenario family (`p < 0.05 / 21`), which
requires at least thirteen of fourteen paired gains below the margin.
Publication also requires every measured and headroom generator run below the
85% physical-core CPU-capacity ceiling;
logical-thread utilization is retained separately and SMT never inflates the
denominator.
Quartiles, individual gains, CPU usage, and exact environments remain in the
raw record; a noisy median alone is not mistaken for a scaling signal. Each
unique run writes a durable `running` manifest before execution and atomically
checkpoints scenario evidence. Handled contract failures transition it to
`failed`; sudden process termination can leave the truthful `running` status.
Both states and all completed evidence remain under
`bench/results/runs/<run-id>/raw/`. A failed contract, changed artifact,
incomplete scenario, missing headroom plateau, or CPU-headroom failure preserves
the canonical `bench/results/raw/` records and SVGs unchanged.

The public comparison deliberately uses the classic Uvicorn install and pins it
to stdlib asyncio plus h11. Non-WebSocket cells explicitly use `--ws none`;
`websockets` is installed separately and selected only for the WebSocket cell,
not through `uvicorn[standard]`. h2corn explicitly uses uvloop,
two Tokio runtime threads, and one Python loop thread. Hypercorn uses uvloop,
while Gunicorn uses its first-party ASGI worker with uvloop and the Python HTTP
parser. The raw identity labels these profiles and records installed
protocol/event-loop package versions, including missing optional packages.

## Candidate versus control (`compare.py`)

Use `compare.py`, rather than the plotting command, to decide whether a
code change improves performance. It accepts separately named control and
candidate server commands. A fixed seed chooses the first lead order,
after which blocks alternate AB/BA. Every block therefore runs both
variants close together while balancing which one runs first; this helps
cancel thermal, frequency, and background drift.

The runner performs complete warmup blocks before recording an even number of
measured pairs (ten by default), so control and candidate lead exactly the same
number of retained blocks. Each freshly started process in a measured pair
first receives one second of unmeasured traffic over the exact load path;
the response and worker-PID set are revalidated before measurement. Every oha
invocation has a wall timeout
derived from its requested duration. On Linux, the runner samples cumulative
CPU and aggregate resident memory for both complete server and load-generator
process groups. The default gate rejects a run when the generator averages
more than 85% of its pinned physical-core capacity or process accounting is
unavailable. Logical-CPU utilization is retained separately, and selected CPU
topology/policy state is part of the frozen comparison identity. Every current
benchmark-driver thread is pinned to the management CPU before that identity is
captured; later monitor threads inherit the same Linux affinity.
Server, load-generator, and management CPUs must be supplied together and be
disjoint at both the logical-thread and physical-core levels. The load set must
contain every online, allowed SMT sibling of each selected physical core.
Pinned comparisons apply the publication harness's role-aware host-noise gates
to every retained run: two one-second quiet probes immediately before load,
then fixed one-second and whole-load activity checks across CPUs not assigned to
server, load, or management roles. The raw run retains every quiet probe and
the complete during-load activity record; crossing the 10% aggregate or 15%
single-logical-CPU limit aborts the comparison. Runs without explicit CPU roles
remain useful for development, but their summaries are marked
`DIAGNOSTIC_UNPINNED` and are never labelled evidence-grade.

The evidence JSON retains the unmodified oha output, commands, lead
orders, warmups, measured samples, environment and source-tree
fingerprint, process resource samples, throughput, and p50/p99/p99.9
latency. The summary reports the median paired percentage delta and a
seeded paired-bootstrap 95% confidence interval. Its empirical noise
spread is the interquartile range of the paired percentage deltas. The
runner labels a result `STABLE_ABOVE_IQR` only when the bootstrap interval
excludes zero and the median effect is larger than that observed spread.
This is a descriptive practical-effect gate, not a calibrated hypothesis
test or proof that an effect generalizes beyond the recorded workload and
machine.

Build or install the two source trees into separate environments, bind both
commands to the same address, and give server, load-generator, and management
roles separate physical cores when the machine allows it:

```bash
uv run python bench/compare.py \
  --control 'baseline=/tmp/h2corn-control/bin/h2corn bench.bench_app:app --loop asyncio -b 127.0.0.1:8000' \
  --candidate 'candidate=/tmp/h2corn-candidate/bin/h2corn bench.bench_app:app --loop asyncio -b 127.0.0.1:8000' \
  --url http://127.0.0.1:8000/ \
  --duration 10s \
  --warmups 2 \
  --trials 10 \
  --server-cpus 2 \
  --load-cpus 8-15,24-31 \
  --management-cpus 0 \
  --expected-workers 1 \
  --expected-body 'Hello, World!'
```

Arguments after each `NAME=` value are parsed with shell-style quoting,
but no shell is invoked. Add `--http2` for direct HTTP/2 and use
`--method`, `--body`, and repeatable `--header` arguments reproduce a
specific request. HTTP loads require either an exact UTF-8 `--expected-body`
or an exact `--expected-body-sha256` plus `--expected-body-size`; the harness
checks it before and after load over the measured protocol. Direct HTTP/2 uses
prior knowledge for cleartext and requires ALPN `h2` for TLS; falling back to
HTTP/1.1 fails the contract. `--expected-workers` likewise requires the
same complete set of distinct worker PIDs before and after load. WebSocket
loads require every session to echo its exact nonce. For a standalone `ws://`
or `wss://` load, readiness defaults to HTTP or HTTPS on the same origin;
`--ready-url` accepts only an explicit HTTP(S) endpoint. `--disable-keepalive`
isolates new-connection HTTP/1
costs such as TLS handshakes; it is intentionally incompatible with
HTTP/2. Each run writes an incrementally durable raw record under
`bench/results/compare/`.

The runner pins the HTTP protocol explicitly (`1.1` unless `--http2` is
selected), including under TLS where ALPN otherwise makes an omitted
version ambiguous. Frozen identity covers the harness, app and load scripts,
load-tool executables, request and measurement settings, runtime environment,
source state, TLS inputs, and each variant's executable, imported package,
native extension, and protocol/runtime dependency trees. Before each server
launch and after each paired block, a cheap stat fingerprint of every hashed
identity input is re-checked; the full identity (hashes, dependency probes,
git, system state) is recomputed whenever that fingerprint changes and always
at suite completion.

Do not treat the single observed public plots as paired code-change evidence. Record
CPU topology, affinity, and governor setup alongside any published conclusion.

## End-to-end candidate matrix (`matrix.py`)

`matrix.py` expands the paired runner across the declarative scenarios in
`matrix.toml`: HTTP/1 TCP and Unix sockets, direct HTTP/2, TLS with both
ALPN protocols, unary and streaming bodies, pathsend files, WebSockets,
and the 1/4-worker by 1/4-loop topology. The default command
intentionally runs only a short HTTP/1 unary case; use selection globs
during development and reserve the complete 52-scenario matrix for
release evidence.

Pass base server commands without bind, worker, loop-thread, or TLS
arguments; the matrix supplies those per scenario. Reuse an
`--output-dir` to resume an interrupted matrix. A result is skipped only
when its schema and complete comparison identity match exactly, so
changing a command, workload, topology, affinity, duration, or seed
automatically reruns it.
The matrix record and each scenario entry propagate the comparison's host-noise
mode. Strict pinned limits produce `pinned-noise-gated`; relaxed pinned limits
are explicitly `diagnostic-pinned-noisy`; missing CPU roles are
`diagnostic-unpinned`.

```bash
# Inspect capability skips and the expanded scenario IDs.
uv run python bench/matrix.py \
  --control 'baseline=/tmp/control/bin/python -m h2corn bench.bench_app:app' \
  --candidate 'candidate=/tmp/candidate/bin/python -m h2corn bench.bench_app:app' \
  --list

# Fast default and a focused HTTP/2 dry run.
uv run python bench/matrix.py \
  --control 'baseline=/tmp/control/bin/python -m h2corn bench.bench_app:app' \
  --candidate 'candidate=/tmp/candidate/bin/python -m h2corn bench.bench_app:app'
uv run python bench/matrix.py \
  --control 'baseline=/tmp/control/bin/python -m h2corn bench.bench_app:app' \
  --candidate 'candidate=/tmp/candidate/bin/python -m h2corn bench.bench_app:app' \
  --select 'h2-tcp-*-w1-l1' --dry-run

# Full release matrix on a 32-thread Ryzen 9 5950X. Server and load roles own
# complete, physically disjoint SMT cores; CPU 0 is reserved for management.
uv run python bench/matrix.py \
  --control 'baseline=/tmp/control/bin/python -m h2corn bench.bench_app:app' \
  --candidate 'candidate=/tmp/candidate/bin/python -m h2corn bench.bench_app:app' \
  --full --runtime-gil disabled \
  --server-cpus 2-9,18-25 \
  --load-cpus 1,10-15,17,26-31 --management-cpus 0 \
  --output-dir bench/results/matrix/release-candidate
```

Adapt those CPU IDs from `lscpu -e=CPU,CORE` on other hosts; disjoint logical
IDs alone do not prove physical-core isolation. The largest matrix topology is
four workers times four loop threads, so its server set needs 16 execution
lanes if that cell is intended to run without CPU oversubscription.

`--runtime-gil auto` follows the interpreter running the harness. When
the control and candidate commands use other interpreters, select
`enabled` or `disabled` explicitly so four-loop capability is not
inferred from the wrong runtime. Unsupported scenarios are recorded with
their reason instead of being silently omitted. HTTP loads require oha,
WebSocket loads require k6, and TLS certificate generation uses the
development dependency `trustme`.
TLS material is retained in the output directory and reused on resume; changing
it invalidates the frozen comparison identity instead of silently mixing runs.

## Component comparisons

Choose the narrowest harness that still includes the production
mechanism:

| Surface | Evidence |
| ------- | -------- |
| WebSocket masking | `mask_kernel_compare.py`; imports the production body and checks declared workload lengths, phases, and alignment |
| HPACK | Header-heavy HTTP/2 matrix cells; report table storage and allocation evidence beside timing |
| Python calls | Unary/streaming matrix cells plus interpreter bytecode/C-API inspection; add a microprobe only when it calls the production boundary directly |
| Parser, frame writer, bridge | Focused `matrix.py` cells plus allocation, syscall, layout, future-size, and stack evidence |
| Lifecycle | Repeated startup, cancellation, failure, and shutdown integration tests with FD, reference, task, and RSS evidence |

Do not benchmark copied parser or in-memory writer helpers: they erase
buffer ownership, syscalls, partial writes, TLS framing, flow control,
and scheduling. Likewise, a scalar-kernel win must be reconciled with a
paired end-to-end cell.

The masking runner discards complete warmup blocks, then uses an even number of
strict alternating AB/BA pairs and paired-bootstrap intervals. With
`--management-cpu`, it pins the harness away from the measurement CPU, freezes
topology and CPU policy, and rejects ambient or during-run CPU interference.
Without that role, output is explicitly diagnostic.

The weighted headline is a declared workload model, not a production
trace. Always retain the per-length cells, and replace the checked-in
weights with an observed deployment distribution before using that
aggregate as a capacity claim.

```bash
uv run python bench/mask_kernel_compare.py \
  --control-kernel eager-key \
  --cpu 2 --management-cpu 0 --warmups 2 --trials 16
```
