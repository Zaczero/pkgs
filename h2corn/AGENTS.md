# h2corn engineering notes

- Public cross-server benchmarks use the classic Uvicorn install, explicitly
  selecting stdlib asyncio and h11. Install `websockets` separately only for the
  WebSocket scenario; do not benchmark `uvicorn[standard]`.
- Publish plots only from the complete frozen eight-trial suite with scenario
  warmup, balanced server order, exact pre/post response gates, complete worker
  readiness, process-group resource evidence, and load-generator headroom.
  Smoke, partial, resumed-incompatible, and failed runs are diagnostic only.
- Keep load-generator affinity fixed during headroom A/B tests. Vary explicit
  oha Tokio workers or k6 `GOMAXPROCS`; changing the cpuset changes runtime
  topology and can also perturb the server's cache/power domain. On this 5950X,
  server processes are pinned to CPUs 2-5 on CCD0 and load runs on CCD1 CPUs
  8-15,24-31.
- Normalize load-generator CPU saturation to selected physical cores, not SMT
  threads. Public runs must freeze and revalidate online/allowed CPUs, SMT and
  LLC topology, governor/driver/EPP, boost, kernel command line, and THP state.
- Pin every benchmark-driver thread, including orchestration/resource sampling,
  to management CPU 0 and retain every thread-affinity mask. Reject a public
  trial when its post-warmup `/proc/stat` probe exceeds 10% physical-core/LLC
  utilization or 15% on one logical CPU; apply the same limits to fixed
  one-second exposure and the full-load aggregate for unused server siblings/LLC
  CPUs during measured and headroom loads. Retain a subsecond final sample raw,
  normalize it to one second for gating, and retain running/failed checkpoints.
- Benchmark docs (README, docs/benchmarks.md) are user-facing: lead with the
  plots and concrete headline numbers (RPS, p99) taken from the canonical
  SVG/raw publication, and refresh those figures whenever the plots are
  republished. Keep deep methodology/evidence-contract detail in
  bench/README.md, not the public pages.
- Prefer reusable public `TypedDict` contracts for ASGI scopes and messages.
  Keep the deliberately broad framework-compatibility callable aliases separate
  from the precise wire types.
- Validate both the regular interpreter and CPython free-threaded build. The
  latter requires
  `_PYTHON_SYSCONFIGDATA_NAME=_sysconfigdata_t_linux_x86_64-linux-gnu` and
  `UV_PROJECT_ENVIRONMENT=.venv-t` on this development host.
