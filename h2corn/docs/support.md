# Support

## Issues, questions, and feature requests

The [GitHub issue tracker](https://github.com/Zaczero/pkgs/issues) is the
home for everything from bug reports to design discussions. Reports get
triaged and rolled into the next release.

!!! tip "What makes a useful report"
    `h2corn --version`, the relevant CLI flags or `h2corn.toml`, the
    reverse proxy in front (if any), and a minimal reproducer. Issues
    with these land faster.

For feature requests, describe the use case rather than the
implementation you have in mind — that leaves the most room for the
right shape of fix.

## Security disclosures

Use GitHub's [private vulnerability reporting](https://github.com/Zaczero/pkgs/security/advisories/new)
for potentially exploitable issues. Do not file these on the public
tracker.

## Premium support

Paid engineering support is available for teams running `h2corn` in
production — typically including the surrounding Python application,
since the server is rarely the only moving part.

Common scopes:

- **Pre-production review** — reverse proxy, listener, TLS, supervisor,
  and resource-limit configuration before going live.
- **Migration** from `uvicorn`, `hypercorn`, or `gunicorn`, with a
  rollback plan.
- **Performance audit** — profiling under realistic load, identifying
  the bottleneck, and applying fixes in code or configuration.
- **Prioritized fixes and features** on a defined timeline.

Reach out at [monicz.dev](https://monicz.dev/#get-in-touch).
