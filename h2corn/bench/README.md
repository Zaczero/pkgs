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

One command, no flags to remember:

```bash
uv run python bench/bench.py --publish
```

Servers run out of the box, unpinned — the harness never limits how much
parallelism a solution uses internally. Only the measurement instrument is
pinned, with roles auto-derived from the host topology: the harness driver
takes the boot domain's first core and the load generator owns every thread
of the largest other last-level-cache domain; oha/k6 worker counts follow the
load CPU count. Explicit `--load-cpus/--management-cpus` (and `--server-cpus`
for deliberately confined experiments) remain as overrides. Without `--publish`, everything stages under a
unique run directory, so smoke tests and filtered runs cannot overwrite the
canonical plots.

Trial counts adapt instead of being fixed: each scenario runs at least three
rotation-balanced cold-start trials per server and keeps going (up to eight)
only while any server's trial-to-trial spread (IQR/median) is still above 10%.
The whole suite is capped by `--time-budget` (default 15 minutes): the
scheduler splits the remaining budget across remaining scenarios and shrinks
the per-trial load duration (3 s base, 1 s floor) to fit. Minimum trials are
never skipped — on a budget too small for the fixed per-trial cost the suite
runs slightly long rather than publishing thin evidence. Each raw record notes its trial count and why the scenario stopped
(`stable`, `max-trials`, or `time-budget`).

The run's `identity.json` records the harness settings, scenario matrix,
server commands and profiles, package versions, git head, and the captured
instrument CPU topology. Every benchmark-driver thread is pinned to the
separate management CPU, including orchestration and 20 Hz resource sampling.
These are pinned instrument roles on a shared development host — the harness
does not gate on ambient host activity; the pinned generator, medians over
rotated trials, and retained per-trial ranges carry the noise story instead.
Keep the tree still while a run measures. Every measured run
requires all configured workers to answer, runs one second of unmeasured
request-path load on the same server process before measurement, and verifies
the exact HTTP body or WebSocket echo before warmup and after measurement.
Warmup commands, raw output, and resource usage are retained alongside all
throughput/latency samples through p99.9 and their ranges. After the scenario
sweep, the single fastest cell gets a quick load-generator headroom check —
one warmup pair plus one reduced/full/full/reduced block comparing 75% versus
100% generator workers on the identical CPU set (oha via
`TOKIO_WORKER_THREADS`, k6 via `GOMAXPROCS`). Publication flags the run when
the median paired gain reaches 5% (the generator, not the server, may be the
bottleneck) or when any measured or headroom generator run exceeds the 85%
physical-core CPU-capacity ceiling; logical-thread utilization is retained
separately and SMT never inflates the denominator. Paired gains, CPU usage,
and exact environments remain in the raw record. Each scenario record is
written once at scenario completion under
`bench/results/runs/<run-id>/raw/`; an interrupted run just gets re-run. A
failed contract, incomplete scenario, missing headroom plateau, or
CPU-headroom failure preserves the canonical `bench/results/raw/` records and
SVGs unchanged.

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
unavailable. Logical-CPU utilization is retained separately. Every current
benchmark-driver thread is pinned to the management CPU before measurement;
later monitor threads inherit the same Linux affinity.
Server, load-generator, and management CPUs must be supplied together and be
disjoint at both the logical-thread and physical-core levels. The load set must
contain every online, allowed SMT sibling of each selected physical core.
Runs without explicit CPU roles remain useful for development.

The result JSON retains the unmodified oha output, commands, lead
orders, warmups, measured samples, process resource samples, throughput,
and p50/p99/p99.9 latency. The summary reports the median paired percentage delta and a
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
HTTP/2. Each run writes its raw record under `bench/results/compare/`.

The runner pins the HTTP protocol explicitly (`1.1` unless `--http2` is
selected), including under TLS where ALPN otherwise makes an omitted
version ambiguous. The stored comparison identity is the measurement
settings themselves: variant names and commands, request shape, duration,
concurrency, trials, warmups, seed, and CPU roles. Keep the two variant
builds still while a comparison runs.

Do not treat the single observed public plots as paired code-change evidence.

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
when its complete comparison identity matches exactly, so
changing a command, workload, topology, affinity, duration, or seed
automatically reruns it.

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
it invalidates the stored comparison identity instead of silently mixing runs.

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
`--management-cpu`, it pins the harness away from the measurement CPU and
freezes topology and CPU policy. Without that role, output is explicitly
diagnostic.

The weighted headline is a declared workload model, not a production
trace. Always retain the per-length cells, and replace the checked-in
weights with an observed deployment distribution before using that
aggregate as a capacity claim.

```bash
uv run python bench/mask_kernel_compare.py \
  --control-kernel eager-key \
  --cpu 2 --management-cpu 0 --warmups 2 --trials 16
```
