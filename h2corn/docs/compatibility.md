---
description: Supported runtimes, wheel compatibility, and upgrading h2corn.
---

# Python compatibility and wheels

h2corn supports standard CPython 3.11 and newer, and free-threaded CPython 3.14t
and newer. Optimized, interpreter-specific wheels are published for supported
standard and free-threaded builds. Generic stable-ABI wheels carry both the
standard `abi3` and free-threaded `abi3t` tags, beginning with CPython
3.15/3.15t. Wheels are available for Linux, macOS, and Windows.

## Upgrading

Patch releases preserve documented behavior except for bug and security fixes.
Minor releases may add APIs and options. Incompatible changes wait for a major
release, normally after at least one minor release with a deprecation warning
that names the replacement.

Configuration field names, TOML keys, environment variables, and CLI flags are
public interfaces. Unknown `H2CORN_*` variables are rejected so a misspelled or
removed setting cannot silently fall back to its default.

`Config` and `RequestPolicy` support pickle using named fields. New releases
read released pickle formats; older releases need not read newer formats.
Only load pickles from trusted sources.

Install an upgrade as a complete package and restart the supervisor.
Application reload does not upgrade the server or re-read its configuration;
see [reload behavior](deployment/operations.md#what-sighup-reloads).
