| Server | Median RPC/s | Blocks | Median completed RPCs/trial | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Connect / h2corn | 17,001 | 6 | 17,051 | 3.558 | 9.965 | 65.2 | - |
| Connect / Hypercorn | 3,409 | 6 | 3,435 | 18.284 | 27.633 | 37.5 | slower |
| grpc.aio | 6,102 | 6 | 6,165 | 10.318 | 13.365 | 56.2 | slower |
| grpclib | 5,250 | 6 | 5,304.5 | 11.919 | 15.363 | 29.9 | slower |
