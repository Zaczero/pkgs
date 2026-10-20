---
description: How h2corn cross-server benchmarks are measured and what workloads they cover.
hide:
  - navigation
---

# Benchmarks

These benchmarks compare h2corn, Uvicorn, Hypercorn, and Gunicorn serving the
same Starlette application, over loopback and a 50 ms round-trip network profile.
Each result identifies the measured runtime, server versions, network profile,
payload, concurrency, and fresh-start trial count. Read the rate, latency,
and memory measures together: a network-limited workload can have similar
throughput across servers despite different processing costs.

--8<-- "bench/results/_generated/benchmark-results.md"

## Methodology {#measurement-method}

Connections go directly to each server, without a reverse proxy or TLS.
Each trial checks status, content type, response body, and worker readiness
before and after load, including a second HTTP/1.1 request on a persistent
connection when the server keeps it open. A response-validation failure retains its diagnostics
and appears as a failed cell without numeric timing or ratios. Configuration,
artifact-identity, load-generator, or deadline failures stop the run. A complete
attempted matrix containing failed response cells is labelled accordingly;
it is not a complete passing comparison.
All servers use standard asyncio. HTTP/1.1 uses Uvicorn's h11 parser and
Gunicorn's Python HTTP parser. This controlled profile does not rank their
optional native-parser or uvloop configurations. HTTP/2 uses Uvicorn's
experimental zttp backend and Gunicorn's h2 backend in explicit prior-knowledge
mode; the exact backend versions and requested limits are retained with
the results. Settings without an exposed equivalent are recorded rather
than silently described as equal. Every HTTP/2 trial also records the peer's
effective SETTINGS after a PING round trip, including settings changes and
initial connection credit. This exposes requested overrides that a backend
does not actually retain on the wire.
TCP listeners are shared by inheritance and have `TCP_NODELAY` enabled for
every server, avoiding a listener-construction difference in asyncio's
accepted-socket handling. Hypercorn's per-connection request-retirement limit
is raised beyond the bounded trial workload; its default of 1,000 requests
otherwise closes connections during the sustained multiplexing case.

Trials use fresh server starts, rotated server order, and no CPU pinning.
Each result states its warmup and load durations. Published rate
ranges are the observed minimum and maximum across trials; they are not
confidence intervals. Rate comparisons resample paired trial blocks and report
the median h2corn-to-competitor rate ratio with a per-comparison 95% bootstrap
interval. An interval including 1 leaves the ordering unresolved; it does not
establish equivalent performance. Latencies are medians of the per-trial
percentiles, not pooled percentiles. Load is closed-loop, so these are not
latency guarantees under an independent arrival rate.

The 8 MiB network upload uses a fixed batch of sixteen completed requests on
eight connections, after eight warmup requests. These uploads can outlast a
short time window with a small HTTP/2 receive window. Batch throughput counts
all sixteen completions over the actual elapsed time, rather than counting
cancelled attempts or declaring valid long uploads to be zero throughput.

Peak proportional set size (PSS) is the maximum sampled simultaneous sum
across unique supervisor and worker processes. PSS shares mapped-page cost
between processes; one-second sampling can miss shorter peaks. Missing PSS,
a failed correctness probe, or a measurement failure cannot become a numeric
comparison result. Raw load-generator output, trial order, sampled memory,
worker module identities, and source/wheel identities accompany the results.
Memory sampling covers measured load after warmup.

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
