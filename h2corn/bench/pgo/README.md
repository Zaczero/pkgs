# h2corn PGO corpus

This adapter trains the installed production CLI and supervisor on GIL-enabled
CPython 3.15, targeting the x86-64 manylinux wheel. The shared
[distribution builder](../../../tools/pgo.md) owns builds, profile validation,
merging, and release integration.

The corpus is a chosen synthetic operation mix, not a claim about observed
deployment traffic. It gives HTTP application traffic most of the weight while
retaining distinct transfer, streaming, file, WebSocket, RPC, and failure paths:

| Workload family | Useful-operation share |
| --- | ---: |
| Plain HTTP application traffic, Unix sockets, and PROXY protocol | 59.4% |
| TLS application traffic, including mutual TLS | 16% |
| Uploads and downloads | 10% |
| Server-sent events | 3% |
| Files and ranges | 4% |
| WebSocket messages, plain and compressed, over WS and WSS | 3% |
| HTTP/2 early hints, cleartext and TLS | 0.2% |
| HTTP/1 response trailers, cleartext and TLS | 0.2% |
| WebSocket denial responses over WS and WSS | 0.002% |
| gRPC unary, client-streaming, server-streaming, and duplex calls | 4% |
| Application failures | 0.198% |

`corpus.py` defines the leaves, denominators, and expected native subsystems.
Requests, completed event records, and echoed WebSocket messages are the useful
units; each RPC leaf counts completed calls while preserving its per-call
message count. The mixture includes synchronous and asynchronous Starlette
endpoints, asyncio and uvloop, one and multiple workers, variable headers and
bodies, HTTP TLS 1.2 and 1.3 with verified session resumption, secure WebSocket
handshakes, and pipe and regular-file
logging sinks. Connections remain open until graceful drain where necessary to
include completed native logging work.

The weights normalize by useful work, not elapsed training time or leaf count.
Ordinary HTTP receives 75.4% of that mix. WebSocket message shares are split
across WS/WSS and plain/compressed sessions; each session exchanges 64 messages.
The denial share therefore represents roughly four rejected handshakes per
hundred accepted sessions, rather than competing with messages as though each
message opened a connection. Streaming RPC weights count complete calls with
their message sequences intact. Larger calls naturally contribute more native
execution; these shares are not percentages of CPU time.

All advertised ASGI extensions have explicit serving paths: file and range
workloads exercise `pathsend` and `zerocopysend`; HTTP/1 and gRPC workloads
verify response trailers; HTTP/2 clients verify 103 responses and their Link
fields; rejected WebSocket handshakes verify denial status and streamed body.
HTTPS and WSS handlers check the TLS scope. These are representative paths,
not every combination of protocol settings.

Fork workers reset their own image's counters before native worker startup and
dump after normal worker teardown. The collection-only overlay records worker
generations, image identities, successful dumps, and supervisor reaps. Client
and application completion counts must agree, every worker must contribute,
and enabled access logs must account for the completed requests. Parent and
unlisted raw profiles never enter the merge.

The generator uses atomic counter updates. Its private native collection
functions and Python supervisor overlay are absent from the final wheel.
Persistent idle native threads mean the terminal dump is an approximate
snapshot rather than a stop-the-world profile.

Keep evaluation separate from these fixtures: the existing raw, Starlette,
WebSocket, and Connect gRPC benchmarks exercise distinct serving shapes.
Include held-out application work, startup, large transfers, and rate-controlled
latency when qualifying a changed mixture.

The [local qualification report](qualification.md) records measured gains,
inconclusive cases, artifact identities, and the remaining hosted-build check.
