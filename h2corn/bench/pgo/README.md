# h2corn PGO corpus

This adapter trains the installed production CLI and supervisor on GIL-enabled
CPython 3.15, targeting the x86-64 manylinux wheel. The shared
[distribution builder](../../../tools/pgo.md) owns builds, profile validation,
merging, and release integration.

The corpus represents plaintext proxy hops, encrypted internal hops with and
without client certificates, and direct public TLS termination. Its synthetic
weights prioritize ordinary serving traffic while covering transfers, streaming,
files, WebSockets, RPCs, and error paths.

| Workload family | Useful-operation share |
| --- | ---: |
| Plaintext proxy traffic, including Unix sockets and PROXY protocol | 32% |
| TLS proxy traffic with server authentication | 16% |
| Mutual-TLS proxy traffic | 8% |
| Direct TLS application traffic, including mutual TLS | 19.4% |
| Uploads and downloads | 10% |
| Server-sent events | 3% |
| Files and ranges | 4% |
| WebSocket messages, plain and compressed, over WS and WSS | 3% |
| HTTP/2 early hints, cleartext and TLS | 0.2% |
| HTTP/1 response trailers, cleartext and TLS | 0.2% |
| WebSocket denial responses over WS and WSS | 0.002% |
| gRPC unary, client-streaming, server-streaming, and duplex calls | 4% |
| Application failures | 0.198% |

`corpus.py` defines the workloads, useful-work denominators, and expected native
subsystems. Weights normalize by verified HTTP requests, event records, WebSocket
messages, or complete RPCs with their message sequences intact. Each WebSocket
session exchanges 64 messages; the denial share corresponds to about four
rejected handshakes per hundred accepted sessions. These are chosen workload
shares, not observed deployment frequencies or CPU-time percentages.

The application mix includes synchronous and asynchronous Starlette endpoints,
asyncio and uvloop, one and multiple workers, and variable headers and bodies.
Encrypted proxy hops exercise forwarded identity and PROXY protocol over TLS;
mutual-TLS handlers verify the client certificate in the ASGI scope. HTTP TLS
1.2 and 1.3 exercise session resumption. Access logging uses both pipes and
regular files, with completed logging work included before graceful shutdown.

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

The [qualification report](qualification.md) records measured gains, footprint
costs, and inconclusive latency results.
