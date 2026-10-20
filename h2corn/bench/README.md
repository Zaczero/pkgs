# Reproduce the benchmarks

Compare h2corn, Uvicorn, Hypercorn, and Gunicorn with the same Starlette
application. The [benchmark page](../docs/benchmarks.md) explains the workloads
and how to read the results.

Run from the `h2corn` checkout on Linux with CPython 3.15, readable
`/proc/<pid>/smaps_rollup`, and the project's Rust build environment.
Install `oha` for HTTP scenarios and `k6` for WebSockets. Build and install an
explicit release wheel, then keep that installation for the measurement:

```bash
uv sync --upgrade --python 3.15
WHEELS="$(mktemp -d)"
uv run --no-sync python bench/bench.py --build-wheel "$WHEELS"
uv pip install --python .venv/bin/python --reinstall "$WHEELS"/*.whl
uv run --no-sync python bench/bench.py --wheel "$WHEELS"/*.whl
```

Results are saved under `bench/results/runs/` as per-trial load-generator
JSON, process and module identities, sampled PSS, SVG charts, and matching
data tables. The installed h2corn bytes must match the supplied wheel.
Every CLI must belong to the same interpreter environment. Each trial checks
all worker identities and verifies the response before and after load.

## Select a workload

```bash
uv run --no-sync python bench/bench.py --servers h2corn uvicorn --types h1
```

Use `--duration`, `--warmup-duration`, `--max-trials`, `--scenario-budget`,
and `--suite-budget` to control measurement effort. A short smoke run checks
that the setup works; it is not a performance comparison:

```bash
uv run --no-sync python bench/bench.py \
  --servers h2corn --types h1 --network-profile loopback \
  --duration 1s --warmup-duration 1s --max-trials 3 \
  --scenario-budget 60 --suite-budget 120
```

The 8 MiB network-upload case instead uses sixteen completed requests on
eight connections, after eight warmup requests. Its phase deadline is
120 seconds; `--duration` does not shorten that fixed batch.

The full matrix uses eight fixed trial blocks. Allow several hours for a
complete run; scenario and suite budgets are upper bounds rather than an
estimate of normal runtime. Pilot runs establish the trial count before
the final measurements, without stopping when a preferred interval appears.

The default suite measures loopback, then tries selected streaming and
multiplexing scenarios with 50 ms round-trip latency in a network namespace.
That profile needs `unshare`, `ip`, `tc`, and user/network namespace support.
The qdisc is confined to the throwaway namespace; the host network is not
changed. If unavailable, it is skipped with a reason; loopback results are
not substituted.

## Comparison conditions

- Servers use the same application, asyncio loop, aligned settings, and access
  logging to `/dev/null`.
- Server order is rotated; neither servers nor load generators are CPU-pinned.
- Every inherited TCP listener has `TCP_NODELAY` enabled. Hypercorn's
  per-connection request cap is raised beyond the trial workload; all exposed
  overrides and unmatched settings are retained in each result.
- Results report median rates, individual trial observations, and observed
  ranges across fresh starts. Rate-ratio intervals resample complete paired
  blocks, retaining their server order.
- Sampling uses a fixed configured trial count. Response-validation failures
  remain explicit failed cells with no numeric rate or ratio; later blocks
  do not repeat an already verified failure. Infrastructure, identity,
  load-generator, and deadline failures prevent publication.
- All four selected releases support the suite's cleartext protocols.
  HTTP/1 uses h11 for Uvicorn and the Python parser for Gunicorn; HTTP/2 uses
  zttp and h2 respectively. Optional accelerated HTTP/1 configurations are
  outside this comparison.
- Maximum sampled PSS sums unique supervisor and worker PIDs. Missing
  accounting is an error, never zero memory.
- HTTP/2 trials retain the effective peer SETTINGS after a PING round trip,
  independently of the requested CLI limits.
- Listeners remain owned by the harness and are inherited by each server;
  ports, Unix sockets, and load-generator files are private to the trial.

To replace checked-in results, add `--publish` to a complete run with
`--wheel`. It replaces raw records, retained evidence, tables, and both chart
themes together. Partial server/type/profile selections cannot publish.

See `uv run python bench/bench.py --help` for the full option list.

## gRPC workloads

The optional gRPC family compares the same generated Protobuf messages and
Python handler work through Connect on h2corn and Hypercorn, grpc.aio, and a
representative grpclib subset. It requires regular CPython 3.15, Go 1.26 or
newer, `curl`, and `openssl`. The optional development group supplies Buf and
the Python RPC runtimes:

```bash
uv sync --upgrade --group grpc-interop --python 3.15
```

Build and install the explicit h2corn wheel as above, then qualify the eight
workloads without running a timing comparison:

```bash
uv run --no-sync python bench/bench.py --family grpc --qualify-only \
  --wheel "$WHEELS"/*.whl
```

Qualification checks complete responses, status and metadata, duplex progress,
handler settlement after cancellation, and framework input validation. Results
retain failed qualifications separately from successful-call measurements.
HTTP 200 alone is not success.

Omit `--qualify-only` for the twelve-block comparison. `--rpc-cases U32 Duplex`
selects a smaller apparatus check. The full matrix includes zero-serialized,
tiny and bulk unary messages, upload, download, duplex, and representative
verified TLS workloads. The client validates every message and terminal outcome.
Streaming plots use verified response messages per second; upload and unary
plots use complete RPCs per second. Latency percentiles are upper bounds from
1%-width client histogram buckets, summarized across trials.

The gRPC family uses one serving process and one standard asyncio loop per
arm, identity compression, and no access logging. It retains native runtime
threads in process accounting, records actual client connections, and annotates
throughput with maximum sampled serving-process PSS. Partial matrices cannot
publish. `--family grpc --publish` replaces only `bench/results/grpc`; the
existing HTTP results remain intact.
