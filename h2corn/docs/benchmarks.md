---
description: How h2corn cross-server benchmarks are measured and what workloads they cover.
hide:
  - navigation
---

# Benchmarks

**h2corn delivered 11–29× higher throughput** in the one-worker HTTP/1.1 GET
comparison, using standard asyncio and pure-Python parsers for Uvicorn and Gunicorn.

These benchmarks compare h2corn, Uvicorn, Hypercorn, and Gunicorn serving the
same Starlette application, over loopback and a 50 ms round-trip network profile.
Results cover throughput, latency, and memory.

--8<-- "bench/results/_generated/benchmark-results.md"

## Methodology {#measurement-method}

Connections go directly to each server, without a reverse proxy or TLS.
Each trial checks status, content type, response body, and worker readiness
before and after load, including a second HTTP/1.1 request on a persistent
connection when the server keeps it open. Failed response checks produce a
`failed` cell, with diagnostics retained in the raw evidence.
All servers use standard asyncio. HTTP/1.1 uses Uvicorn's h11 parser and
Gunicorn's Python HTTP parser. HTTP/2 uses cleartext prior knowledge.
Requested limits and effective peer SETTINGS are retained in the raw evidence.
TCP listeners are shared by inheritance and have `TCP_NODELAY` enabled for
every server. Hypercorn's per-connection request-retirement limit is raised
beyond the trial workload.

Trials use fresh server starts, rotated server order, and no CPU pinning.
There are eight fixed trial blocks, with two seconds of warmup and ten seconds
of measured load per trial. Rates are medians; dots show individual trials and
whiskers show their observed range. Faster/slower classifications use paired
rate ratios with per-comparison 95% bootstrap intervals, retained in the raw
records. An interval including 1 is inconclusive. Latencies are medians of
per-trial percentiles. Clients use closed-loop load.

The 8 MiB network upload uses a fixed batch of sixteen completed requests on
eight connections, after eight warmup requests. Throughput counts successful
completions over actual elapsed time.

Peak proportional set size (PSS) is the maximum sampled simultaneous sum
across unique supervisor and worker processes. PSS shares mapped-page cost
between processes. One-second sampling covers measured load after warmup and
can miss shorter peaks.

The [raw scenario records](https://github.com/Zaczero/pkgs/tree/main/h2corn/bench/results/raw)
and [per-trial evidence](https://github.com/Zaczero/pkgs/tree/main/h2corn/bench/results/evidence)
retain the commands, observations, and identities behind the figures.

### Workloads {#workloads-and-limits}

- HTTP/1.1 and HTTP/2 GETs, including a multiplexed HTTP/2 case with ten
  concurrent streams on one connection.
- GET over Unix domain sockets.
- A 128 KiB Starlette `FileResponse`, using the ASGI
  [`http.response.pathsend`](asgi.md#sending-a-file) extension where supported.
- Streaming downloads that yield eight 16 KiB chunks.
- POSTs that upload 1 KiB, then receive three response chunks with 20 ms of
  application delay.
- An 8 MiB HTTP/2 upload on the shaped network, with the complete upload
  verified before and after load.
- HTTP/1.1 WebSocket sessions that connect, echo one text message, and
  disconnect. They measure session turnover, not sustained message throughput.
