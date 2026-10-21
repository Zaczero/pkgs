# Reproduce the benchmarks

Compare h2corn, Uvicorn, Hypercorn, Gunicorn, and the selected gRPC stacks with
the same benchmark applications. The [benchmark page](../docs/benchmarks.md)
explains the published workloads and how to read their results.

Run from the `h2corn` checkout on Linux with CPython 3.15, readable
`/proc/<pid>/smaps_rollup`, and the project's Rust build environment. The full
HTTP matrix also requires `oha`, `k6`, `unshare`, `ip`, `tc`, and working user
and network namespaces. Build and install an explicit release wheel, then keep
that installation for the measurement:

```bash
uv sync --upgrade --python 3.15
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

## Sampling and time budget

Time-window scenarios calibrate each server in one verified serving process.
The harness measures successive 0.5, 1, 2, and 4 second windows, stopping when
adjacent rates agree within 10%. Every server in a scenario then receives the
longest selected duration. If rates have not stabilized by four seconds, that
scenario is marked calibration-limited and its comparisons are inconclusive.
Calibration runs after the correctness probe without a separate load warmup.
Calibration-trial elapsed time minus the requested calibration-window
durations estimates per-cell setup and checks. Time-window production blocks
use a 0.25-second warmup; the fixed-count upload has none. The production
forecast adds the selected duration and applicable warmup.

The harness freezes six fresh-process paired production blocks for every
scenario before collecting production data.

Comparisons use paired production-block rate ratios and distribution-free
order-statistic intervals for the common median of independent paired-block
ratios. Six blocks use the minimum and maximum ratios (96.875% coverage). Each
comparison records its sample size, interval method, and achieved coverage.

One monotonic deadline covers preparation, qualification, calibration,
production, reporting, and cleanup. Thirty minutes is the target and 45 minutes
is the hard total. If the forecast cannot fit the complete six-block matrix
within the hard limit, the run retains its diagnostic evidence, exits nonzero,
and does not publish.

For a smaller diagnostic collection, select a family and workload explicitly:

```bash
uv run --no-sync python bench/bench.py \
  --family http --servers h2corn uvicorn --types h1 \
  --network-profile loopback --wheel "$WHEELS"/*.whl
```

Selectors reduce the collected scenarios or servers; they do not change the
adaptive sampling policy. Diagnostic runs cannot replace published results.

## Comparison conditions

- Server order is rotated within paired blocks. Servers and load generators
  are not CPU-pinned.
- HTTP servers use the same application, standard asyncio, aligned settings,
  and access logging to `/dev/null`. HTTP/1 uses h11 for Uvicorn and the Python
  parser for Gunicorn; HTTP/2 uses zttp and h2 respectively.
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
- The 8 MiB HTTP/2 upload completes sixteen requests on eight connections.
  The handler checks each received body against its declared length, and the
  load generator requires all responses to succeed and verifies their byte
  total. Small-body correctness probes run before and after measurement. The
  upload batch uses a separate client process with fresh connections, so it
  needs no full-size warmup uploads.

The default run includes loopback and a 50 ms round-trip network profile. The
RTT profile is applied inside a throwaway network namespace; the host network
is not changed. A run without the required namespace tools or capabilities
fails rather than substituting loopback results.

## gRPC workloads

The gRPC family compares common generated Protobuf messages and Python handler
work through Connect on h2corn and Hypercorn, grpc.aio, and a representative
grpclib subset. It requires CPython 3.15, Go 1.26 or newer, `curl`, and
`openssl`. The optional development group supplies Buf and the Python RPC
runtimes:

```bash
uv sync --upgrade --group grpc-interop --python 3.15
```

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
or network-profile selections cannot publish.
