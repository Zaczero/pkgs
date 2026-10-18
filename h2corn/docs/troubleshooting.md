---
description: Diagnose protocol mismatches, proxy identity, startup, TLS, reload, and worker failures.
---

# Troubleshooting

## Protocol mismatch

A connection reset, `400`, `502`, or “invalid HTTP/2 preface” often means the
client and listener disagree on the protocol. Compare:

```bash
curl --http1.1 -v http://127.0.0.1:8000/
curl --http2-prior-knowledge -v http://127.0.0.1:8000/
```

The first requires HTTP/1.1 to be enabled; the second sends cleartext HTTP/2.
For TLS, use `https://` with `curl --http2`. Enable
[`--no-http1`](configuration.md#option-http1) only after the proxy uses h2c
upstream. Browsers need HTTP/1.1 on a cleartext development listener.

## WebSocket 502

If ordinary requests work but WebSocket upgrades fail, check the proxy's
upstream protocol. HTTP/1.1 `Upgrade` cannot be forwarded unchanged over
HTTP/2; that transport needs RFC 8441 extended CONNECT.

The [nginx and Caddy recipes](deployment/proxy.md#nginx) route upgrades over
HTTP/1.1 and ordinary requests over h2c. Keep h2corn's HTTP/1.1 support enabled
for that split route. An HTTP/2-only WebSocket route needs RFC 8441 support in
both the client and proxy.

## Forwarded identity is wrong

If the app sees the proxy address, check all three settings:

- [`--proxy-headers`](configuration.md#option-proxy_headers) enables processing.
- [`--forwarded-allow-ips`](configuration.md#option-forwarded_allow_ips) must
  contain the proxy's source IP or Unix-socket peer.
- [`--forwarded-fields`](configuration.md#option-forwarded_fields) selects
  the fields. It defaults to `for,proto`; select `forwarded` separately for
  the RFC 7239 dialect.

Compare the actual upstream headers and peer address with those settings.
Do not use `*` unless every route to the listener is trusted. See
[proxy headers](deployment/proxy.md#proxy-headers-and-proxy-protocol).

## Bind, UDS, or pidfile failure

Check for an occupied port and incorrect directory permissions:

```bash
ss -ltnp 'sport = :8000'
```

For a Unix socket or pidfile, inspect its path and parent directory. Remove a
stale socket only after confirming no live listener owns it.
[`--uds-permissions`](configuration.md#option-uds_permissions) controls the
socket's mode, not its parent directory.

An embedded [`Server`][h2corn.Server] cannot combine `pid` with `user` or
`group`; use the CLI or [`serve()`][h2corn.serve] for pidfiles with privilege
changes.

## Lifespan startup or shutdown failure

Inspect the application's lifespan traceback. It may have rejected startup,
failed to finish dependency initialization, or exceeded a
[lifespan timeout](configuration.md#option-timeout_lifespan_startup).
`lifespan=on` requires support; `auto` allows an app to decline it.

Use `--check-config` to distinguish configuration and TLS errors from app
import or lifespan failures. If you increase shutdown timeouts, also update
the service manager's [stop budget](deployment/operations.md#shutdown-budget).

## Crash loop

Start with the **first worker traceback**, not the final backoff message.
Run your target in the foreground with `--workers 1 --log-format json
--no-access-log` under the service user.

Fix import, dependency, or lifespan errors before restarting. Increasing restart
limits will not resolve a deterministic failure. If a worker stops responding
after serving successfully, inspect its healthcheck log and look for blocking
application code.

## TLS or ALPN failure

Check the certificate and negotiated protocol on the direct TLS listener:

```bash
# Replace example.com with the certificate's hostname.
openssl s_client -connect 127.0.0.1:8443 -alpn h2 -servername example.com </dev/null
```

Direct TLS requires an unencrypted PEM certificate/key pair and a TCP listener.
Behind a TLS-terminating proxy, check the HTTPS edge and h2c upstream separately.
`--no-http1` advertises only `h2`, so HTTP/1.1-only clients cannot connect.

Replacing PEM files requires a restart; SIGHUP does not reload them.
See [Direct TLS](deployment/tls.md).

## Reload changed the wrong thing

SIGHUP reloads **application code only**. TOML, environment variables, env-files,
listeners, limits, and TLS material stay as they were at startup.

Validate changed configuration with `--check-config`, then restart the
supervisor. The development `--reload` file watcher is separate from
[rolling SIGHUP reloads](deployment/operations.md#what-sighup-reloads).

## `uvloop` is missing

Install `h2corn[uvloop]` in the same environment as h2corn, or choose
[`--loop asyncio`](configuration.md#option-loop). `--loop auto` uses uvloop
when installed and otherwise uses the standard-library loop.
Windows and some free-threaded combinations lack a usable uvloop package.

## systemd notify does not become ready

h2corn sends `READY=1` only after every worker is serving. Check the worker logs
for startup failures, then inspect the unit (replace `myapp.service`):

```bash
systemctl show myapp.service -p Type -p NotifyAccess -p MainPID -p SubState
journalctl -u myapp.service -b
```

Use `Type=notify`, `NotifyAccess=main`, and an `ExecStart` that makes h2corn the
main process. A shell wrapper must use `exec`. The development `--reload`
watcher is incompatible with notify mode.

See the [systemd unit example](deployment/operations.md#full-systemd-unit).
Readiness notifications do not extend `TimeoutStartSec` or `TimeoutStopSec`.
