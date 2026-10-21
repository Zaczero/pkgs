| Server | Median messages/s | Blocks | Median complete RPC/s | Median completed RPCs/trial | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Connect / h2corn | 71,769 | 6 | 1,121 | 2,246.5 | 6.896 | 10.473 | 68.6 | - |
| Connect / Hypercorn | 16,056 | 6 | 251 | 508 | 33.051 | 43.454 | 38.6 | slower |
| grpc.aio | 34,535 | 6 | 540 | 1,084.5 | 14.616 | 18.932 | 56.6 | slower |
| grpclib | not selected | - | - | - | - | - | - | - |
