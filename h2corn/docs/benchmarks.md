# Benchmarks

Synthetic benchmarks comparing `h2corn` against the common Python ASGI
servers across the workloads that matter: small JSON GETs, file
serving, streaming, and WebSockets.

The harness lives in [`bench/`](https://github.com/Zaczero/pkgs/tree/main/h2corn/bench)
and uses [k6](https://k6.io/) as the load generator against the same
Starlette application served by each of `h2corn`, `uvicorn`,
`hypercorn`, and `gunicorn`.

!!! note "Local results"
    The numbers below are from a single development machine —
    representative of relative ordering on a quiet Linux host, not a
    promise about your hardware. Re-run `python bench/bench.py` to
    measure your own environment.

## Methodology

Each scenario runs the **same Starlette application** behind every
server, so any difference in throughput or latency comes from the
server itself — accept loop, framing, routing, framework overhead —
not from the application code.

| Knob                     | Value                                                          |
| ------------------------ | -------------------------------------------------------------- |
| Load generator           | [k6](https://k6.io/) (one process, separate from the server)   |
| Iterations per scenario  | 200 000                                                        |
| Concurrent VUs           | 100 (1 000 for streaming POST)                                 |
| Workers                  | 1 and 4, side-by-side per scenario                             |
| Transports               | HTTP/1.1 over TCP, HTTP/1.1 over UDS, HTTP/2, WebSocket        |
| Server side              | `h2corn`, `uvicorn`, `hypercorn`, `gunicorn` (uvicorn workers) |
| Reverse proxy            | None — k6 talks directly to each server                        |
| Servers compared on HTTP/2 | `h2corn` and `hypercorn` only (the other two don't speak HTTP/2) |

Each server starts cold per scenario and is given a moment to settle
before k6 begins; both processes run on the same host with no CPU
pinning or governor tuning. The plots show a single observed run —
intended to surface relative ordering at a glance, not to publish
production capacity guidance.

Hardware and kernel for the displayed run are recorded as a header on
each plot. To reproduce on your own box, see
[Reproducing](#reproducing).

## Headline result

The most representative workload — a small JSON GET endpoint served by
four workers — looks like this:

![HTTP/1 GET, 4 workers](assets/benchmarks/benchmark_http_1_get_4_workers.svg)

`h2corn` reaches **~90k RPS at p99 2.3 ms**, several times ahead of the
nearest mainstream alternative on the same deployment shape and the
same Starlette app.

## HTTP/1 GET

=== "1 worker"

    ![HTTP/1 GET, 1 worker](assets/benchmarks/benchmark_http_1_get_1_worker.svg)

=== "4 workers"

    ![HTTP/1 GET, 4 workers](assets/benchmarks/benchmark_http_1_get_4_workers.svg)

## HTTP/1 GET over Unix domain sockets

=== "1 worker"

    ![HTTP/1 GET over UDS, 1 worker](assets/benchmarks/benchmark_http_1_get_over_uds_1_worker.svg)

=== "4 workers"

    ![HTTP/1 GET over UDS, 4 workers](assets/benchmarks/benchmark_http_1_get_over_uds_4_workers.svg)

## HTTP/2 GET

Only `h2corn` and `hypercorn` accept HTTP/2 directly.

=== "1 worker"

    ![HTTP/2 GET, 1 worker](assets/benchmarks/benchmark_http_2_get_1_worker.svg)

=== "4 workers"

    ![HTTP/2 GET, 4 workers](assets/benchmarks/benchmark_http_2_get_4_workers.svg)

## Static file

=== "HTTP/1, 1 worker"

    ![HTTP/1 static file, 1 worker](assets/benchmarks/benchmark_http_1_static_file_1_worker.svg)

=== "HTTP/1, 4 workers"

    ![HTTP/1 static file, 4 workers](assets/benchmarks/benchmark_http_1_static_file_4_workers.svg)

=== "HTTP/2, 1 worker"

    ![HTTP/2 static file, 1 worker](assets/benchmarks/benchmark_http_2_static_file_1_worker.svg)

=== "HTTP/2, 4 workers"

    ![HTTP/2 static file, 4 workers](assets/benchmarks/benchmark_http_2_static_file_4_workers.svg)

## Streaming POST

=== "HTTP/1, 1 worker"

    ![HTTP/1 streaming POST, 1 worker](assets/benchmarks/benchmark_http_1_streaming_post_1_worker.svg)

=== "HTTP/1, 4 workers"

    ![HTTP/1 streaming POST, 4 workers](assets/benchmarks/benchmark_http_1_streaming_post_4_workers.svg)

=== "HTTP/2, 1 worker"

    ![HTTP/2 streaming POST, 1 worker](assets/benchmarks/benchmark_http_2_streaming_post_1_worker.svg)

=== "HTTP/2, 4 workers"

    ![HTTP/2 streaming POST, 4 workers](assets/benchmarks/benchmark_http_2_streaming_post_4_workers.svg)

## WebSocket

=== "1 worker"

    ![HTTP/1 WebSocket, 1 worker](assets/benchmarks/benchmark_http_1_websocket_1_worker.svg)

=== "4 workers"

    ![HTTP/1 WebSocket, 4 workers](assets/benchmarks/benchmark_http_1_websocket_4_workers.svg)

## Reproducing

```bash
git clone https://github.com/Zaczero/pkgs.git
cd pkgs/h2corn
uv sync
uv run python bench/bench.py
```

The harness records per-scenario k6 summaries and renders the SVGs you
see above. Output lands in `bench/results/`.
