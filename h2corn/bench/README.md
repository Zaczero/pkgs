# Reproduce the benchmarks

Compare h2corn, Uvicorn, Hypercorn, Gunicorn, and the selected gRPC stacks with
the same benchmark applications. The [benchmark page](../docs/benchmarks.md)
explains the published workloads and how to read their results.

Run from the `h2corn` checkout on Linux with CPython 3.15, readable
`/proc/<pid>/smaps_rollup`, and the project's Rust build environment. The full
HTTP matrix also requires `oha` and `k6`; gRPC requires Go 1.26 or newer,
`curl`, and `openssl`. Install both families' dependencies, build a release
wheel, and keep that installation for the measurement:

```bash
uv sync --upgrade --group grpc-interop --python 3.15
WHEELS="$(mktemp -d)"
uv run --no-sync python bench/bench.py --build-wheel "$WHEELS"
uv pip install --python .venv/bin/python --reinstall "$WHEELS"/*.whl
uv run --no-sync python bench/bench.py --wheel "$WHEELS"/*.whl
```

The default run measures HTTP and gRPC together and stages its evidence under
`bench/results/runs/`. It records the installed wheel identity, benchmark
commands, process identities, load-generator output, sampled PSS, plots, and
accessible tables. Each measured worker is checked against the installed
artifacts, and each trial verifies its response contract before and after load.

Wheel builds use the same `release-dist` builder and source receipts as
[PGO](pgo/README.md). To evaluate a PGO wheel, install it and pass its path with
`--wheel`; the harness verifies the installed bytes and retains its build receipt.
It also records the current harness source independently, so an older baseline
wheel can be compared without pretending it was built from the current checkout.

## Sampling and time budget

Each scenario calibrates a common duration, then collects six paired
fresh-process blocks. Unresolved comparisons may extend to twelve blocks
within the collection budget. The
[methodology](../docs/benchmarks.md#measurement-method) explains calibration,
warmup, stopping rules, and uncertainty intervals.

One monotonic deadline covers preparation, qualification, calibration,
production, reporting, and cleanup. Thirty minutes is the target and 45 minutes
is the hard total. If the forecast cannot fit the complete six-block matrix
within the hard limit, the run retains its diagnostic evidence, exits nonzero,
and does not publish.

For a smaller diagnostic collection, select a family and workload explicitly:

```bash
uv run --no-sync python bench/bench.py \
  --family http --servers h2corn uvicorn --types h1 \
  --wheel "$WHEELS"/*.whl
```

Selectors reduce the collected scenarios or servers; they do not change the
adaptive sampling policy. Diagnostic runs cannot replace published results.

## Comparison conditions

- Server order is rotated within paired blocks. Servers and load generators
  are not CPU-pinned.
- HTTP servers use the same application, standard asyncio, aligned settings,
  and access logging to `/dev/null`. HTTP/1 uses h11 for Uvicorn and the Python
  parser for Gunicorn. Their selected releases do not support the cleartext
  prior-knowledge HTTP/2 mode used by this comparison.
- Every inherited TCP listener has `TCP_NODELAY` enabled. Hypercorn's
  per-connection request cap is raised beyond the trial workload; effective
  settings and unmatched overrides are retained in the evidence.
- Reported maximum sampled PSS sums unique supervisor and worker PIDs for
  production blocks. The same one-second sampler surrounds calibration windows
  so its observer cost enters the forecast; calibration PSS is not aggregated
  into results. Missing accounting is an error, never zero; short peaks may
  fall between samples.
- HTTP/2 trials retain the effective peer SETTINGS after a PING round trip.
  Listeners, sockets, and load-generator files are private to each trial.

HTTP connections use loopback. Results describe local client/server work,
not latency across a production network.

## gRPC workloads

The gRPC family compares common generated Protobuf messages and Python handler
work through Connect on h2corn and Hypercorn, grpc.aio, and a representative
grpclib subset. The `grpc-interop` group installed above supplies Buf and the
Python RPC runtimes.

The complete gRPC collection is included in the default combined run. To
diagnose qualification without measuring throughput:

```bash
uv run --no-sync python bench/bench.py --family grpc --qualify-only \
  --wheel "$WHEELS"/*.whl
```

Qualification checks complete responses, status and metadata, duplex progress,
handler settlement after cancellation, and framework input validation. HTTP
200 alone is not success. `--rpc-cases U32 Duplex` selects a smaller diagnostic
subset. The full matrix includes zero-serialized, tiny and bulk unary messages,
upload, download, duplex, and verified TLS workloads. Streaming plots use
verified response messages per second; upload and unary plots use complete RPCs
per second. Latency percentiles are upper bounds from one-percent client
histogram buckets.

The gRPC family uses one serving process and one standard asyncio loop per arm,
identity compression, and no access logging. It retains native runtime threads
in process accounting, records actual client connections, and annotates
throughput with maximum sampled serving-process PSS.

## Publishing

Only a complete, combined HTTP and gRPC run with the identified release wheel
can replace checked-in benchmark results:

```bash
uv run --no-sync python bench/bench.py \
  --wheel "$WHEELS"/*.whl --publish
```

Publication occurs only after the complete matrix, frozen plan, retained
qualification, and evidence pass their gates. Partial family, server, scenario,
or gRPC case selections cannot publish.

When a newer comparator dependency is incompatible, `--comparator-selection`
accepts a JSON record naming the selected version and every newer stable
release rejected, with a reason and a SHA-256-verified failure-evidence file
for each. This evidence accompanies the complete matrix; it does not select
a smaller workload.
