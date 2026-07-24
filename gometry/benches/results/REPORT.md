# Gometry v1.0.0 benchmark evidence

No benchmark is currently locked: the previous exploratory artifacts measured
an API and harness that changed during pre-release cleanup. The final baseline
must come from the release worktree.

## Canonical commands

```bash
.venv/bin/python benches/drivers/bench.py --profile smoke
.venv/bin/python benches/drivers/bench.py --profile release --plan-only
.venv/bin/python benches/drivers/bench.py --profile release \
  --output-dir benches/results/baseline
.venv/bin/python benches/support/summarize_bench.py \
  benches/results/baseline/<timestamp>-release.json --format md
```

The single manifest is `benches/support/_bench_registry.py`:

| Profile | Rows | Sampling | Whole-run cap |
|---|---:|---|---:|
| `smoke` | 15 | single debug value | 420 s |
| `release` | 62 | 6 processes per row; pairs split evenly across A/B and B/A lead order | 2,400 s |

The driver writes raw pyperf JSON plus one run-level JSON and Markdown report,
stops after the first failed command, and terminates the whole subprocess group on
timeout. A non-plan release run fails before starting any timer when the doctor
reports contention, missing dependencies, non-performance CPU policy, or an
uncommitted gometry tree, or no kernel-isolated CPU. It also records four
fresh-process tail-latency/process-RSS/Python-allocation probes. Fix the environment
and checks contention again after the last probe. Only a full successful run
with clean preflight and postflight evidence is marked publishable. Fix the
environment instead of publishing a warned run. Release notes must link the final artifacts
and must not publish a comparison until semantic parity, variance, and the
noise floor have been reviewed.

A `--filter` run is explicitly marked as a partial manifest, omits the
whole-profile resource probes, and cannot become the regression gate's release
candidate even when every selected row succeeds.
