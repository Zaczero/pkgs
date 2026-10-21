| Server | Median RPC/s | Blocks | Median completed RPCs/trial | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Connect / h2corn | 1,103 | 6 | 1,108.5 | 7.247 | 10.423 | 68.9 | - |
| Connect / Hypercorn | 244 | 6 | 248 | 32.400 | 38.567 | 38.0 | slower |
| grpc.aio | 1,115 | 6 | 1,119 | 7.211 | 9.867 | 151.2 | faster |
| grpclib | not selected | - | - | - | - | - | - |
