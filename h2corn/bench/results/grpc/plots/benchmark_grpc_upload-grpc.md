| Server | Median RPC/s | Median completed RPCs/trial | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs Connect / h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| Connect / h2corn | 1,056 | 10,565 | 7.504 | 10.631 | 71.4 | - |
| Connect / Hypercorn | 227 | 2,276.5 | 34.737 | 43.671 | 39.4 | slower |
| grpc.aio | 1,077 | 10,777 | 7.393 | 10.318 | 198.8 | inconclusive |
| grpclib | not selected | - | - | - | - | - |
