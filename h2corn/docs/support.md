---
description: Where to file bugs and security reports, and what commercial support covers.
---

# Support

## Issues, questions, and feature requests

Bug reports, questions, feature requests, and design discussions go to the
[GitHub issue tracker](https://github.com/Zaczero/pkgs/issues).

Issue reports include `h2corn` [`--version`](configuration.md#command-version), the relevant CLI flags or
`h2corn.toml`, any reverse proxy in front, and a minimal reproducer.

## Security disclosures

GitHub's [private vulnerability reporting](https://github.com/Zaczero/pkgs/security/advisories/new)
is the channel for potentially exploitable issues; the public tracker is not.

## Premium support

Paid engineering support is available for teams running `h2corn` in
production, covering the surrounding Python application as well as the
server.

Common scopes:

- **Pre-production review** — reverse proxy, listener, TLS, supervisor,
  and resource-limit configuration before going live.
- **Migration** from `uvicorn`, `hypercorn`, or `gunicorn`, with a
  rollback plan.
- **Performance audit** — profiling under realistic load, identifying
  the bottleneck, and applying fixes in code or configuration.
- **Prioritized fixes and features** on a defined timeline.

Reach out at [monicz.dev](https://monicz.dev/#get-in-touch).
