| Server | Median RPC/s | Blocks | Median completed RPCs/trial | p50 latency (ms) | p99 latency (ms) | PSS (MB) | Throughput vs h2corn |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Connect / h2corn | 7,162 | 6 | 7,175 | 2.185 | 3.385 | 65.1 | - |
| Connect / Hypercorn | 1,221 | 6 | 1,231.5 | 12.779 | 17.659 | 40.5 | slower |
| grpc.aio | 3,860 | 6 | 3,873 | 4.110 | 5.568 | 66.2 | slower |
| grpclib | 2,046 | 6 | 2,057 | 8.007 | 13.501 | 32.5 | slower |

Collection: direction-resolved.
