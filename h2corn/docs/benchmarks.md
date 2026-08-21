---
description: h2corn against uvicorn, hypercorn and gunicorn across GETs, Unix sockets, static files, streaming and WebSockets.
---

# Benchmarks

The checked-in run compares `h2corn` with `uvicorn`, `hypercorn`, and `gunicorn`
on the same Starlette application across HTTP/1.1, HTTP/2, Unix sockets, static
files, streaming, and WebSockets. It used Python 3.14.3 on Linux 6.18.38,
x86_64, and an AMD Ryzen 9 5950X.

The load generator connects directly to each server with no reverse proxy or
TLS terminator. Trials are 2 seconds of warmup followed by 10 seconds of load,
with 3-9 cold starts, rotated server order, and no CPU pinning. Reported values
are medians with the observed range. Each trial checks status, content type,
body, and worker readiness; a competitor that cannot serve a scenario is
excluded with its reason, while an h2corn failure stops the run.

The run uses loopback and records peak proportional set size (PSS) across the
supervisor and workers. The harness requirements, optional network profile,
smoke command, raw per-run output, and publish rules are in
[`bench/README.md`](https://github.com/Zaczero/pkgs/tree/main/h2corn/bench).

## HTTP/1 GET

![HTTP/1 GET, 1 worker: requests per second and peak memory (PSS) by server.](assets/benchmarks/benchmark_http_1_get_1_worker.svg)

![HTTP/1 GET, 4 workers: requests per second and peak memory (PSS) by server; h2corn 242,313 RPS at p99 0.821 ms, gunicorn 47,414 RPS at p99 4.218 ms.](assets/benchmarks/benchmark_http_1_get_4_workers.svg)

## HTTP/1 GET over Unix domain sockets

![HTTP/1 GET over UDS, 1 worker: requests per second and peak memory (PSS) by server.](assets/benchmarks/benchmark_http_1_get_over_uds_1_worker.svg)

![HTTP/1 GET over UDS, 4 workers: requests per second and peak memory (PSS) by server.](assets/benchmarks/benchmark_http_1_get_over_uds_4_workers.svg)

## HTTP/2 GET, multiplexed

Ten concurrent streams share a connection in this workload. Hypercorn returned
connection errors and is excluded from the comparison.

![HTTP/2 GET multiplexed, 1 worker: requests per second and peak memory (PSS) by server.](assets/benchmarks/benchmark_http_2_get_multiplexed_1_worker.svg)

## HTTP/2 GET

The direct HTTP/2 workload includes h2corn and Hypercorn; Uvicorn and Gunicorn
provide no HTTP/2 listener in this harness.

![HTTP/2 GET, 1 worker: requests per second and peak memory (PSS) by server.](assets/benchmarks/benchmark_http_2_get_1_worker.svg)

![HTTP/2 GET, 4 workers: requests per second and peak memory (PSS) by server.](assets/benchmarks/benchmark_http_2_get_4_workers.svg)

## Static file

The workload serves a 128 KiB Starlette `FileResponse`, using the ASGI
`http.response.pathsend` extension where supported.

![HTTP/1 static file, 1 worker: requests per second and peak memory (PSS) by server.](assets/benchmarks/benchmark_http_1_static_file_1_worker.svg)

![HTTP/1 static file, 4 workers: requests per second and peak memory (PSS) by server.](assets/benchmarks/benchmark_http_1_static_file_4_workers.svg)

![HTTP/2 static file, 1 worker: requests per second and peak memory (PSS) by server.](assets/benchmarks/benchmark_http_2_static_file_1_worker.svg)

![HTTP/2 static file, 4 workers: requests per second and peak memory (PSS) by server.](assets/benchmarks/benchmark_http_2_static_file_4_workers.svg)

## Streaming download

The workload yields eight 16 KiB chunks.

![HTTP/1 streaming download, 1 worker: requests per second and peak memory (PSS) by server.](assets/benchmarks/benchmark_http_1_portable_streaming_download_1_worker.svg)

![HTTP/1 streaming download, 4 workers: requests per second and peak memory (PSS) by server.](assets/benchmarks/benchmark_http_1_portable_streaming_download_4_workers.svg)

![HTTP/2 streaming download, 1 worker: requests per second and peak memory (PSS) by server.](assets/benchmarks/benchmark_http_2_portable_streaming_download_1_worker.svg)

![HTTP/2 streaming download, 4 workers: requests per second and peak memory (PSS) by server.](assets/benchmarks/benchmark_http_2_portable_streaming_download_4_workers.svg)

## Streaming POST

The workload uploads a 1 KiB body from 1,000 concurrent clients and receives a
chunked response. Gunicorn timed out 281 requests in the single-worker HTTP/1
scenario and is excluded.

![HTTP/1 streaming POST, 1 worker: requests per second and peak memory (PSS) by server.](assets/benchmarks/benchmark_http_1_streaming_post_1_worker.svg)

![HTTP/1 streaming POST, 4 workers: requests per second and peak memory (PSS) by server.](assets/benchmarks/benchmark_http_1_streaming_post_4_workers.svg)

![HTTP/2 streaming POST, 1 worker: requests per second and peak memory (PSS) by server.](assets/benchmarks/benchmark_http_2_streaming_post_1_worker.svg)

![HTTP/2 streaming POST, 4 workers: requests per second and peak memory (PSS) by server.](assets/benchmarks/benchmark_http_2_streaming_post_4_workers.svg)

## WebSocket

![HTTP/1 WebSocket, 1 worker: WebSocket sessions per second and peak memory (PSS) by server.](assets/benchmarks/benchmark_http_1_websocket_1_worker.svg)

![HTTP/1 WebSocket, 4 workers: WebSocket sessions per second and peak memory (PSS) by server.](assets/benchmarks/benchmark_http_1_websocket_4_workers.svg)

Reruns, focused A/B measurements, and the complete non-publishing harness
command are documented in [`bench/README.md`](https://github.com/Zaczero/pkgs/tree/main/h2corn/bench).
The workload excludes proxy and TLS behavior, HTTP/3, browser behavior,
cold-start latency, failure recovery, and applications other than the tested
Starlette app.
