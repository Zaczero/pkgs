| Server | Median messages/s | Blocks | Median complete RPC/s | Median completed RPCs/trial | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs Connect / h2corn |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Connect / h2corn | 68,655 | 12 | 1,073 | 10,733 | 7.284 | 10.898 | 70.4 | - |
| Connect / Hypercorn | 15,622 | 12 | 244 | 2,445 | 33.884 | 45.444 | 41.2 | slower |
| grpc.aio | 32,765 | 12 | 512 | 5,124.5 | 15.439 | 20.502 | 58.8 | slower |
| grpclib | not selected | - | - | - | - | - | - | - |
