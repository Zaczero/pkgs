| Server | Median RPC/s | Median completed RPCs/trial | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs Connect / h2corn |
| --- | --- | --- | --- | --- | --- | --- |
| Connect / h2corn | 16,843 | 168,474 | 3.684 | 7.176 | 67.6 | - |
| Connect / Hypercorn | 3,206 | 32,100.5 | 19.603 | 32.562 | 42.6 | slower |
| grpc.aio | 6,007 | 60,141 | 10.525 | 13.907 | 59.7 | slower |
| grpclib | 5,080 | 50,871.5 | 12.219 | 16.388 | 38.9 | slower |
