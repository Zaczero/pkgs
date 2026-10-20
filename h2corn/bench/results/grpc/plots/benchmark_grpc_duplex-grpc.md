| Server | Median messages/s | Median complete RPC/s | Median completed RPCs/trial | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs Connect / h2corn |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Connect / h2corn | 68,590 | 1,072 | 10,723.5 | 7.320 | 10.953 | 64.0 | - |
| Connect / Hypercorn | 17,672 | 276 | 2,766 | 29.776 | 42.176 | 38.0 | slower |
| grpc.aio | 33,214 | 519 | 5,195.5 | 15.362 | 20.097 | 57.7 | slower |
| grpclib | 19,486 | 304 | 3,048 | 26.031 | 33.552 | 31.0 | slower |
