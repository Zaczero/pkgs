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
before and after load. A failed response or load trial retains its diagnostics
and prevents publishing an incomplete comparison.
All servers use standard asyncio. HTTP/1.1 uses Uvicorn's h11 parser and
Gunicorn's Python HTTP parser. This controlled profile does not rank their
optional native-parser or uvloop configurations. HTTP/2 uses Uvicorn's
experimental zttp backend and Gunicorn's h2 backend in explicit prior-knowledge
mode; the exact backend versions and applicable limits are retained with
the results. Settings without an exposed equivalent are recorded rather
than silently described as equal.
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

Peak proportional set size (PSS) is the maximum sampled simultaneous sum
across unique supervisor and worker processes. PSS shares mapped-page cost
between processes; one-second sampling can miss shorter peaks. Missing PSS,
a failed correctness probe, or a measurement failure prevents publication
of that comparison. Raw load-generator output, trial order, sampled memory,
worker module identities, and source/wheel identities accompany the results.

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
