# h2corn engineering notes

- Public cross-server benchmarks use the classic Uvicorn install, explicitly
  selecting stdlib asyncio and h11. Install `websockets` separately only for the
  WebSocket scenario; do not benchmark `uvicorn[standard]`.
- Benchmark methodology, budgets, trial policy, affinity, and publication rules
  live in [`bench/README.md`](bench/README.md). Do not duplicate them here —
  that file is the owning source; regenerate plots and headline numbers only
  from a complete `bench.py --publish` run as described there.
- Prefer reusable public `TypedDict` contracts for ASGI scopes and messages.
  Keep the deliberately broad framework-compatibility callable aliases separate
  from the precise wire types.
- Validate both the regular interpreter and CPython free-threaded build. The
  latter requires
  `_PYTHON_SYSCONFIGDATA_NAME=_sysconfigdata_t_linux_x86_64-linux-gnu` and
  `UV_PROJECT_ENVIRONMENT=.venv-t` on this development host.
