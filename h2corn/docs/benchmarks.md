---
description: h2corn against uvicorn, hypercorn and gunicorn across GETs, Unix sockets, static files, streaming and WebSockets.
---

# Benchmarks

Synthetic benchmarks comparing `h2corn` against the common Python ASGI
servers across the workloads that matter: small plaintext GETs, file
serving, streaming, and WebSockets.

The harness lives in [`bench/`](https://github.com/Zaczero/pkgs/tree/main/h2corn/bench)
and drives the same Starlette application served by each of `h2corn`,
`uvicorn`, `hypercorn`, and `gunicorn` with [oha](https://github.com/hatoo/oha)
(HTTP cells) and [k6](https://k6.io/) (WebSocket cells).

!!! note "Local results"
    The plots below are from a single development machine —
    representative of relative ordering on a quiet Linux host, not a
    promise about your hardware. Re-run `python bench/bench.py` to
    measure your own environment.

## Methodology

Each scenario runs the **same Starlette application** behind every
server, so any difference in throughput or latency comes from the
server stack itself — accept loop, framing, routing, event loop —
not from the application code. Access logging is enabled on every
server: that's the deployment shape people actually run.

| Knob                     | Value                                                          |
| ------------------------ | -------------------------------------------------------------- |
| Load generator           | [oha](https://github.com/hatoo/oha) (HTTP), [k6](https://k6.io/) (WebSocket) — separate process from the server |
| Trials                   | 3–9 cold starts per server; bar = median, whisker = observed range |
| Duration per trial       | 2 s warmup, then 10 s of sustained load                        |
| Concurrent VUs           | 100 (1 000 for streaming POST)                                 |
| Workers                  | 1 and 4, side-by-side per scenario                             |
| Transports               | HTTP/1.1 over TCP, HTTP/1.1 over UDS, HTTP/2, WebSocket        |
| Configuration alignment  | Equivalent settings matched across servers; noted below where it changes a result |
| Server side              | `h2corn`, `uvicorn`, `hypercorn`, `gunicorn` (first-party ASGI worker); access logging on |
| Reverse proxy            | None — the load generator talks directly to each server        |
| Servers compared on HTTP/2 | `h2corn` and `hypercorn` only (the other two don't speak HTTP/2) |

Nothing is pinned: every server runs on the whole machine with whatever
parallelism it ships, exactly as a deployment would. Noise is handled in the
statistics instead. Trial order is rotated so no server holds the same position
twice in a row, and a scenario keeps sampling until the winner is clear of the
runner-up with its own median inside a ±3 % confidence interval — or until the
scenario's time ceiling is reached, in which case the plot says so beneath the
chart. Whiskers show the observed range throughout. Every trial verifies the
exact response body and that every configured worker is answering before it
counts, and each plot carries the hardware and kernel it ran on as a header.

Comparisons are per-scenario: a figure quoted against "the fastest alternative"
means the fastest in *that* scenario, not an average across servers. The one
chart without a comparison is HTTP/2 multiplexed, where no other server
completed the workload — read that as a correctness result.

Every published cell is loopback. The harness can also shape traffic through a
network namespace and sample peak memory; neither is published, so neither is
described here.

### Server configuration alignment

Every setting with the same operational meaning is aligned across servers
before a run. Where that alignment moves a result by at least **10%**, this page
reports both medians rather than quietly replacing the default one.

The loop choice is the one that qualifies, and it matters most to `uvicorn`'s
four-worker TCP cell: on the stdlib loop that cell hits a Nagle and delayed-ACK
interaction which `uvloop` avoids, moving it from 2,434 to 29,344 RPS. `h2corn`
moves 232,891 to 244,252 over the same swap, because its I/O runs in Rust and the
loop only schedules application callbacks. `uvicorn[standard]` installs `uvloop`,
so read that bar as the stdlib figure it is.

## Headline result

The most representative workload — a small plaintext GET endpoint served
by four workers — looks like this:

![HTTP/1 GET, 4 workers](assets/benchmarks/benchmark_http_1_get_4_workers.svg)

`h2corn` reaches **~242k RPS at p99 0.8 ms** — about **5×** `gunicorn`'s
first-party ASGI worker, the nearest of the three, on the same deployment
shape and the same Starlette application. Over Unix sockets the same
workload reaches **~271k RPS**, and on HTTP/2 the gap widens to **~21×**.

Two cells are missing a bar, and say so on the plot: `gunicorn` could not
hold 1 000 concurrent streaming POSTs on one worker (281 timeouts), and
`hypercorn` returned connection errors under HTTP/2 multiplexing. A server
that cannot serve a workload is excluded from that scenario, with its reason on
the plot.

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

## HTTP/2 GET, multiplexed

Ten concurrent streams per connection instead of one request at a time —
the reason to speak HTTP/2 in the first place.

`hypercorn` is absent because it returned connection errors under this
workload rather than serving it.

![HTTP/2 GET multiplexed, 1 worker](assets/benchmarks/benchmark_http_2_get_multiplexed_1_worker.svg)

## HTTP/2 GET

Only `h2corn` and `hypercorn` accept HTTP/2 directly.

=== "1 worker"

    ![HTTP/2 GET, 1 worker](assets/benchmarks/benchmark_http_2_get_1_worker.svg)

=== "4 workers"

    ![HTTP/2 GET, 4 workers](assets/benchmarks/benchmark_http_2_get_4_workers.svg)

## Static file

A 128 KiB Starlette `FileResponse`, using the ASGI
`http.response.pathsend` extension where the server supports it.

=== "HTTP/1, 1 worker"

    ![HTTP/1 static file, 1 worker](assets/benchmarks/benchmark_http_1_static_file_1_worker.svg)

=== "HTTP/1, 4 workers"

    ![HTTP/1 static file, 4 workers](assets/benchmarks/benchmark_http_1_static_file_4_workers.svg)

=== "HTTP/2, 1 worker"

    ![HTTP/2 static file, 1 worker](assets/benchmarks/benchmark_http_2_static_file_1_worker.svg)

=== "HTTP/2, 4 workers"

    ![HTTP/2 static file, 4 workers](assets/benchmarks/benchmark_http_2_static_file_4_workers.svg)

## Streaming download

A chunked response the application yields in eight 16 KiB pieces —
server-sent events, log tails, and generated exports have this shape.

=== "HTTP/1, 1 worker"

    ![HTTP/1 streaming download, 1 worker](assets/benchmarks/benchmark_http_1_portable_streaming_download_1_worker.svg)

=== "HTTP/1, 4 workers"

    ![HTTP/1 streaming download, 4 workers](assets/benchmarks/benchmark_http_1_portable_streaming_download_4_workers.svg)

=== "HTTP/2, 1 worker"

    ![HTTP/2 streaming download, 1 worker](assets/benchmarks/benchmark_http_2_portable_streaming_download_1_worker.svg)

=== "HTTP/2, 4 workers"

    ![HTTP/2 streaming download, 4 workers](assets/benchmarks/benchmark_http_2_portable_streaming_download_4_workers.svg)

## Streaming POST

A 1 KiB body uploaded by 1 000 concurrent clients, answered with a
chunked response. `gunicorn` is absent from the single-worker HTTP/1 cell
because it timed out 281 of those requests rather than serving them.

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

The plotting harness drives oha and k6 against each server and renders the
SVGs above. Generated output lands in `bench/results/`; canonical plots are
replaced only by a full `--publish` run
([`bench/README.md`](https://github.com/Zaczero/pkgs/blob/main/h2corn/bench/README.md)).

To run the shape these numbers describe, start with the [Quickstart](quickstart.md)
and [Behind a proxy](deployment/proxy.md).
