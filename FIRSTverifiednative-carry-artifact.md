# First verified native-carry artifact

This record identifies the first production-path artifact that carries geographic source ordinates through exact Q topology and includes the packed `ClipByRect` missing-row validation repair.

## Behavior

Geographic Q nodes retain their authoritative source edge, exact source parameter, and periodic chart displacement. For an eligible interior node, the operation's CRS metric converts that parameter to a fraction of the source edge's ellipsoidal chart-edge arclength before interpolating Z and M. Authentic source endpoints retain their input ordinates. XY-only output, presentation-only requests, and interior axes unavailable at both endpoints do not resolve an interior metric. Periodic rounding keeps the canonical native longitude and its integral lift consistent.

Packed `ClipByRect` arrays with missing or zero rows now enter the bounds-resolving, mask-aware packed operation. Rectangle validation runs once even when no row is present; missing slots remain missing, and valid geographic longitude crossing remains supported.

## Artifact identity

The candidate's source-v5 digest is `6c1c25f91d1b7318fcaadb038c2e720b7b27c82b1412ef0093e77283988a569b`; its native-v5 digest is `8be4d55721d61dbfd2773dbb5a977a02f82400db322babf4a00fb8ff6067e340`.

The matched release baseline A uses source `c8ed5e6286b253174bbb28fc2eec2b0ee47b13b8cb2b72d23ed2c5e908405085`, native digest `8be4d55721d61dbfd2773dbb5a977a02f82400db322babf4a00fb8ff6067e340`, and loaded extension SHA-256 `e81bd11a8a37172e5cfc44409bf764b8898c623ac8c2ba4ff30cae73495cf0b4`. Candidate B's release wheel SHA-256 is `61ab1c51bddb1e01b908401b0ff3d68efd844a440838f230fe774bf67cd4925c`, and its loaded extension SHA-256 is `4d6a7852d10b197146a411a0da72e2e56eeba3ab24efd87932b4208c780be75c`. The candidate development wheel SHA-256 is `22baeccebfe9a1dd60ba5ddd1d42e34a8711453213aa328b4c8bd57e420bb790`, and its loaded extension SHA-256 is `351eba359bd7af2a02c8424c7b1cbee5cbe539314da414cdf8b4dd32308e999d`.

Both release artifacts were built and loaded with rustc `1.101.0-nightly` (`ea137335b`, 2026-10-05), LLVM 23.1.3, LLD 23.1.3, and CPython 3.14.7 on x86-64 Linux. Build metadata confirms the matching target features and native source digest.

## Correctness evidence

- `cargo check --locked -p gometry --all-targets --config 'build.warnings="deny"'` passed.
- The final focused native selection passed 126/126 tests; 2,425 tests were filtered.
- The final development extension passed 15 selected public Q-overlay, ordinate, seam, and huge-longitude nearest/dwithin tests.
- The same extracted final extension passed all 10 `clip_by_rect` tests in `test_constructive_buffer_overlay.py`, including missing, mixed, gathered, empty, seam-crossing, and invalid-bound cases.
- Removing `ClipByRect` from the packed missing-row allowlist produced a compiled wheel whose all-missing invalid-bounds public test failed with `DID NOT RAISE`. The restored source digest is the candidate source-v5 digest above.
- Production source-producer negative controls also compiled and failed their targeted source-correspondence tests: the wrong-branch mutant produced a `360` lift where the test expects `0`, and the missing-seam-adjustment mutant produced `175` where the test expects `180`. The source was restored before final artifact builds.

## Contextual workload costs

Measurements were collected on 2026-10-07. Each A/A and A/B run used 15 measured paired blocks and two warmup blocks. A block measured 4,000 XY Q intersections, 25,000 authentic source-t=1 endpoint intersections, or 12,500 fresh geographic nearest calls. Instructions and cycles were counted on the calling Python thread with non-inherited `perf_event_open`; each event group was fully scheduled. Per-thread CPU and wall time covered the same warmed public-operation loop.

| Workload | Instructions per call, A → B | Paired instruction B/A (95% CI) | Same-artifact A/A B/A (95% CI) | Median wall time per call, A → B |
| --- | ---: | ---: | ---: | ---: |
| XY Q intersection | 2,656,782 → 2,618,020 | 0.985408 [0.985404, 0.985425] | [0.999986, 1.000009] | 239.345 → 235.938 µs |
| Authentic source-t=1 endpoint | 369,126 → 370,387 | 1.003423 [1.003406, 1.003448] | [0.999992, 1.000007] | 40.568 → 40.617 µs |
| Fresh geographic nearest | 828,527 → 840,703 | 1.014696 [1.014695, 1.014708] | [1.000000, 1.000028] | 80.648 → 80.431 µs |

The instruction-count changes are distinct from the same-artifact A/A variation: XY uses fewer instructions, while source-t=1 and nearest use more. Cycle, per-thread CPU, and wall-time confidence intervals include parity for all three workloads. The process retained inherited affinity to CPUs 0–31 on a loaded host, whose recorded load averages ranged roughly from 10 to 17; these timing observations are therefore inconclusive. The measurements are workload-specific context, not a percentage gate or a universal performance claim.
