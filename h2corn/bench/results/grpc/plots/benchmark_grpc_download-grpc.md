| Server | Median messages/s | Blocks | Median complete RPC/s | Median completed RPCs/trial | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs Connect / h2corn |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Connect / h2corn | 19,770 | 12 | 1,236 | 12,363 | 6.432 | 9.341 | 64.1 | - |
| Connect / Hypercorn | 5,090 | 12 | 318 | 3,186 | 24.644 | 39.748 | 51.2 | slower |
| grpc.aio | 17,137 | 12 | 1,071 | 10,715.5 | 7.356 | 10.216 | 56.2 | slower |
| grpclib | not selected | - | - | - | - | - | - | - |
