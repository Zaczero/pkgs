| Server | Median RPC/s | Blocks | Median completed RPCs/trial | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Connect / h2corn | 6,485 | 6 | 6,497 | 2.414 | 3.436 | 64.6 | - |
| Connect / Hypercorn | 1,262 | 6 | 1,272 | 12.466 | 16.734 | 39.8 | slower |
| grpc.aio | 4,010 | 6 | 4,023.5 | 3.932 | 5.298 | 67.0 | slower |
| grpclib | 2,180 | 6 | 2,190.5 | 8.130 | 11.979 | 32.2 | slower |
