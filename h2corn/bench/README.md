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
- Sampling uses a fixed configured trial count. A deadline or execution
  failure retains its evidence and prevents publication of an incomplete
  comparison.
- All four selected releases support the suite's cleartext protocols.
  HTTP/1 uses h11 for Uvicorn and the Python parser for Gunicorn; HTTP/2 uses
  zttp and h2 respectively. Optional accelerated HTTP/1 configurations are
  outside this comparison.
- Maximum sampled PSS sums unique supervisor and worker PIDs. Missing
  accounting is an error, never zero memory.
- Listeners remain owned by the harness and are inherited by each server;
  ports, Unix sockets, and load-generator files are private to the trial.

To replace checked-in results, add `--publish` to a complete run with
`--wheel`. It replaces raw records, retained evidence, tables, and both chart
themes together. Partial server/type/profile selections cannot publish.

See `uv run python bench/bench.py --help` for the full option list.
