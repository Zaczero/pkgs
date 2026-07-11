# Bench harness

Contributor notes for measuring h2corn. The public
[Benchmarks](../docs/benchmarks.md) page shows cross-server plots from a
single observed run; use the tools here when deciding whether a *code
change* improves performance.

| Tool | Purpose |
| ---- | ------- |
| `bench.py` | Cross-server scenarios → SVGs (what the docs plots use) |
| `compare.py` | Paired control vs candidate on one URL/workload |
| `matrix.py` | Expands `compare.py` across `matrix.toml` scenarios |
| `mask_kernel_compare.py` | WebSocket mask kernel microcomparison |

Generated records under `results/` are gitignored.

## Candidate versus control (`compare.py`)

Use `compare.py`, rather than the plotting command, to decide whether a
code change improves performance. It accepts separately named control and
candidate server commands. A fixed seed chooses the first lead order,
after which blocks alternate AB/BA. Every block therefore runs both
variants close together while balancing which one runs first; this helps
cancel thermal, frequency, and background drift.

The runner performs complete warmup blocks before recording at least five
measured pairs (nine by default). Every oha invocation has a wall timeout
derived from its requested duration. On Linux, the runner also samples
aggregate resident memory for each server process group, so worker
processes are included in peak RSS. `--server-cpus` and `--load-cpus`
apply separate `taskset` affinities when the host topology allows it.

The evidence JSON retains the unmodified oha output, commands, lead
orders, warmups, measured samples, environment and source-tree
fingerprint, process resource samples, throughput, and p50/p99/p99.9
latency. The summary reports the median paired percentage delta and a
seeded paired-bootstrap 95% confidence interval. Its empirical noise
floor is the interquartile range of the paired percentage deltas; the
runner calls a result significant only when the interval excludes zero
and the median effect is larger than that floor. This is a measurement
gate, not proof that an effect generalizes beyond the recorded workload
and machine.

Build or install the two source trees into separate environments, bind
both commands to the same address, and pin server and load-generator CPUs
to separate physical cores when the machine allows it:

```bash
uv run python bench/compare.py \
  --control 'baseline=/tmp/h2corn-control/bin/h2corn bench.bench_app:app --loop asyncio -b 127.0.0.1:8000' \
  --candidate 'candidate=/tmp/h2corn-candidate/bin/h2corn bench.bench_app:app --loop asyncio -b 127.0.0.1:8000' \
  --url http://127.0.0.1:8000/ \
  --duration 10s \
  --warmups 2 \
  --trials 9 \
  --server-cpus 2 \
  --load-cpus 4
```

Arguments after each `NAME=` value are parsed with shell-style quoting,
but no shell is invoked. Add `--http2` for direct HTTP/2 and use
`--method`, `--body`, and repeatable `--header` arguments to reproduce a
specific request. `--disable-keepalive` isolates new-connection HTTP/1
costs such as TLS handshakes; it is intentionally incompatible with
HTTP/2. Each run writes an incrementally durable raw record under
`bench/results/compare/`.

The runner pins the HTTP protocol explicitly (`1.1` unless `--http2` is
selected), including under TLS where ALPN otherwise makes an omitted
version ambiguous. Provenance includes hashes of each resolved server
executable and, for direct Python/venv commands, the imported
`h2corn._lib` extension. This prevents a rebuilt editable extension or
wrong virtual environment from being mistaken for the recorded
candidate.

Do not treat the single-run public plots as paired evidence. Record CPU
topology and governor setup alongside any published conclusion.

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

# Full release matrix on a free-threaded build, pinned to separate cores.
uv run python bench/matrix.py \
  --control 'baseline=/tmp/control/bin/python -m h2corn bench.bench_app:app' \
  --candidate 'candidate=/tmp/candidate/bin/python -m h2corn bench.bench_app:app' \
  --full --runtime-gil disabled --server-cpus 2 --load-cpus 4 \
  --output-dir bench/results/matrix/release-candidate
```

`--runtime-gil auto` follows the interpreter running the harness. When
the control and candidate commands use other interpreters, select
`enabled` or `disabled` explicitly so four-loop capability is not
inferred from the wrong runtime. Unsupported scenarios are recorded with
their reason instead of being silently omitted. HTTP loads require oha,
WebSocket loads require k6, and TLS certificate generation uses the
development dependency `trustme`.

## Component comparisons

Choose the narrowest harness that still includes the production
mechanism:

| Surface | Evidence |
| ------- | -------- |
| WebSocket masking | `mask_kernel_compare.py`; imports the production body and checks declared workload lengths, phases, alignment, code size, and optional hardware counters |
| HPACK | Header-heavy HTTP/2 matrix cells; report table storage and allocation evidence beside timing |
| Python calls | Unary/streaming matrix cells plus interpreter bytecode/C-API inspection; add a microprobe only when it calls the production boundary directly |
| Parser, frame writer, bridge | Focused `matrix.py` cells plus allocation, syscall, layout, future-size, and stack evidence |
| Lifecycle | Repeated startup, cancellation, failure, and shutdown integration tests with FD, reference, task, and RSS evidence |

Do not benchmark copied parser or in-memory writer helpers: they erase
buffer ownership, syscalls, partial writes, TLS framing, flow control,
and scheduling. Likewise, a scalar-kernel win must be reconciled with a
paired end-to-end cell.

The masking runner discards complete warmup blocks, then uses strict
alternating AB/BA pairs and paired-bootstrap intervals. `--perf`
additionally records raw cycles, instructions, branches, misses, cache
activity, and L1 data-cache counters when the kernel exposes them. Each
counter is measured alone to avoid hardware-counter multiplexing;
unavailable counters are recorded, while any multiplexed measurement is
rejected.

The weighted headline is a declared workload model, not a production
trace. Always retain the per-length cells, and replace the checked-in
weights with an observed deployment distribution before using that
aggregate as a capacity claim.

```bash
uv run python bench/mask_kernel_compare.py \
  --cpu 2 --warmups 2 --trials 15 --perf --perf-trials 5
```
