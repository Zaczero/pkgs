| Server | Median messages/s | Blocks | Median complete RPC/s | Median completed RPCs/trial | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Connect / h2corn | 20,763 | 6 | 1,298 | 1,304.5 | 6.120 | 8.758 | 62.6 | - |
| Connect / Hypercorn | 5,455 | 6 | 341 | 344 | 23.216 | 28.328 | 53.0 | slower |
| grpc.aio | 18,014 | 6 | 1,126 | 1,130.5 | 7.070 | 9.916 | 53.8 | slower |
| grpclib | not selected | - | - | - | - | - | - | - |
