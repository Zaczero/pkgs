# h2corn engineering notes

- Public cross-server benchmarks use the classic Uvicorn install, explicitly
  selecting stdlib asyncio and h11. Install `websockets` separately only for the
  WebSocket scenario; do not benchmark `uvicorn[standard]`.
- Publish plots only from a complete `bench.py --publish` run: adaptive
  rotation-balanced trials (3-8 per server, stability-stopped, 15-minute suite
  budget), scenario warmup, exact pre/post response gates, complete worker
  readiness, process-group resource evidence, and the fastest-cell headroom
  check. Smoke, partial, and failed runs are diagnostic only. Keep the tree
  still while a run measures — the harness no longer polices mid-run edits.
- Never pin or thread-limit the measured servers — solutions run out of the
  box with whatever parallelism they ship. Only the instrument is pinned
  (auto-derived: harness driver on the boot LLC's first core, load generator
  on every thread of the other LLC). Keep load-generator affinity fixed
  during headroom A/B tests and vary only oha Tokio workers or k6
  `GOMAXPROCS`.
- Normalize load-generator CPU saturation to selected physical cores, not SMT
  threads.
- Pin every benchmark-driver thread, including orchestration/resource sampling,
  to the management CPU. Do not gate on ambient host activity — this is a
  shared development box with editors and other agents running; the pinned
  generator, medians over rotated trials, and retained per-trial ranges carry
  the noise story.
- Results are internal-only (gitignored; only plots and doc numbers are
  published), so keep the harness free of evidence ceremony: no schema
  versions, no per-trial checkpoint writes, no mid-run identity/system
  re-verification, no crash-recovery machinery. Records are written once at
  completion; an interrupted run is simply re-run.
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
