---
description: Diagnose h2corn protocol, proxy, identity, listener, lifespan, TLS, reload, uvloop, and systemd failures.
---

# Troubleshooting

## Protocol mismatch

**Symptom:** A proxy or client reports a connection reset, `400`, `502`, or
"invalid HTTP/2 preface" when h2corn is configured with [`--no-http1`](configuration.md#option-http1).

**Cause:** The peer is speaking HTTP/1.1 to an h2c-only listener, or is using
an HTTP/2 TLS client against a cleartext listener. Cleartext h2c requires an
HTTP/2 prior-knowledge preface; it is not a browser protocol.

**Confirm:** Compare both explicit clients:

```bash
curl --http1.1 -v http://127.0.0.1:8000/
curl --http2-prior-knowledge -v http://127.0.0.1:8000/
```

`curl --http1.1 -v http://127.0.0.1:8000/` works only while HTTP/1.1 is enabled;
`curl --http2-prior-knowledge -v http://127.0.0.1:8000/` is the h2c test.
TLS uses `https://` and `curl --http2`, not prior knowledge.

**Fix:** The proxy's upstream is configured as h2c/prior knowledge before
`--no-http1` is enabled. HTTP/1.1 remains enabled for a direct development
browser or for a proxy that uses HTTP/1.1 WebSocket upgrades.

## WebSocket 502

**Symptom:** Ordinary requests succeed but a WebSocket upgrade returns `502`.

**Cause:** The proxy forwarded an HTTP/1.1 `Upgrade` request to an HTTP/2
upstream without translating it to RFC 8441 extended `CONNECT`. Caddy's
documented h2c path and nginx's HTTP/2 upstream location need a separate
HTTP/1.1 WebSocket route; HAProxy's candidate HTTP/2 path must be verified
with its deployed version.

**Confirm:** Check the proxy log for the upstream protocol and the exact
WebSocket error. Test h2corn directly over HTTP/1.1 with a WebSocket client,
then test the proxy path. Inspect the proxy configuration for an HTTP/1.1
upgrade route and for [`--no-http1`](configuration.md#option-http1) on the h2corn command.

**Fix:** Use the split proxy examples in [Behind a reverse proxy](deployment/proxy.md#nginx):
route upgrades over HTTP/1.1 and keep [`http1`](configuration.md#option-http1) enabled, or use a verified proxy
configuration that translates upgrades to RFC 8441 before enabling
`--no-http1`.

## Forwarded identity is wrong

**Symptom:** The application sees the proxy address instead of the client,
or sees a client-controlled scheme, host, prefix, or IP.

**Cause:** The configured [`--forwarded-fields`](configuration.md#option-forwarded_fields) does not include the fact the
application needs, or the request came from a peer outside
[`--forwarded-allow-ips`](configuration.md#option-forwarded_allow_ips). The default fields are `for` and `proto`; the RFC 7239
`Forwarded` dialect is selected separately.

**Confirm:** Capture the actual upstream headers at the proxy and compare the
peer address with the allowlist. Check `--forwarded-fields` against the headers
the proxy writes. Run a request with a supplied `Forwarded` header from an
untrusted client and verify it is absent from `scope["headers"]` and does not
become application identity.

**Fix:** Trusted forwarded identity requires [`--proxy-headers`](configuration.md#option-proxy_headers), an
`--forwarded-allow-ips` list containing only the proxy's source IP or Unix
socket peer, and `--forwarded-fields` matching the proxy dialect and fields.
Without a trusted proxy, `--proxy-headers` is off. A `*` allowlist trusts every
route to the listener and applies only when every route is inside that trust
boundary.

## Bind, UDS, or pidfile failure

**Symptom:** Startup reports `address already in use`, cannot connect to a
Unix socket, or cannot create/open the pidfile.

**Cause:** Another process owns the TCP port or stale socket path, the Unix
socket directory has the wrong ownership or mode, or the supervisor user
cannot write the pidfile path. A [`Server`][h2corn.Server] embedded in an event loop cannot
combine [`pid`](configuration.md#option-pid) with [`user`](configuration.md#option-user) or [`group`](configuration.md#option-group) because it does not own that ordering.

**Confirm:** Check the listener and filesystem without deleting anything:

```bash
ss -ltnp 'sport = :8000'
stat /run/myapp /run/myapp/h2corn.pid /run/myapp/h2corn.sock
h2corn --check-config --config /etc/myapp/h2corn.toml
```

**Fix:** Stop the process that owns the port, or choose a free bind. Remove a
stale Unix socket only after confirming no live listener owns it; let h2corn
create the path on the next start. Create the pidfile directory with the
service user as owner and use the CLI supervisor for `pid` plus privilege
changes. [`--uds-permissions`](configuration.md#option-uds_permissions) changes the created socket mode, not its parent
directory permissions.

## Lifespan startup or shutdown failure

**Symptom:** The process binds briefly and exits with a lifespan error, or
shutdown reports a timeout after the application tried to clean up.

**Cause:** The app rejected `lifespan.startup`, exceeded
[`timeout_lifespan_startup`](configuration.md#option-timeout_lifespan_startup), or did not complete `lifespan.shutdown` within its
budget. With [`lifespan=on`](configuration.md#option-lifespan), an app that does not support lifespan is a startup
failure; `auto` can continue when the app explicitly declines it.

**Confirm:** Run the same target with [`--log-format`](configuration.md#option-log_format) `json` [`--workers`](configuration.md#option-workers) `1`, inspect
the startup/shutdown event, and compare the configured timeouts with the
application's dependency initialization. Use [`--check-config`](configuration.md#command-check_config) to separate
configuration/TLS errors from application import and lifespan errors.

**Fix:** Make the lifespan context complete its startup and shutdown paths,
or set the documented `lifespan` mode explicitly. Increase the startup or
shutdown timeout only when the dependency operation needs it, and also update
the systemd/Kubernetes combined stop budget described in
[Operations](deployment/operations.md#shutdown-budget).

## Crash loop

**Symptom:** Workers start and disappear repeatedly; the supervisor eventually
stops.

**Cause:** The application import, worker initialization, or lifespan startup
fails repeatedly. The supervisor backs off and stops after repeated failures
when no worker is serving now.

**Confirm:** Inspect the supervisor's first worker traceback and exit code,
not only the final backoff message. Run the target in the foreground with
[`--workers`](configuration.md#option-workers) `1` [`--log-format`](configuration.md#option-log_format) `json` [`--no-access-log`](configuration.md#option-access_log), then run
`h2corn` [`--check-config`](configuration.md#command-check_config) to catch configuration and direct-TLS validation before
importing the app.

**Fix:** Correct the first import/dependency/lifespan error, verify the target
under the service user, and restart the supervisor. Raising healthcheck or
restart limits hides a deterministic startup failure. If only one worker wedges
after serving, inspect the worker-health event and its blocking code.

## TLS or ALPN failure

**Symptom:** TLS negotiation fails, curl selects HTTP/1.1 unexpectedly, or an
HTTP/2 client reports that the server did not advertise `h2`.

**Cause:** The client reached a cleartext listener, the certificate/key pair
does not match, the proxy terminated TLS but the upstream was configured as
TLS instead of h2c, or [`--no-http1`](configuration.md#option-http1)/ALPN choices mismatch the client.

**Confirm:** Inspect the direct listener with:

```bash
openssl s_client -connect 127.0.0.1:8443 -alpn h2 -servername example.com </dev/null
curl -vk --http2 https://127.0.0.1:8443/
```

Check the negotiated ALPN line and h2corn's startup banner. For a proxy,
inspect both hops separately: HTTPS to the edge and h2c prior knowledge to
the application.

**Fix:** Direct TLS requires both unencrypted PEM [`--certfile`](configuration.md#option-certfile) and [`--keyfile`](configuration.md#option-keyfile)
and a TCP listener. [`--no-http1`](configuration.md#option-http1) applies only when every TLS client uses
HTTP/2. TLS files are rotated atomically and h2corn is restarted; SIGHUP does
not rebuild the TLS acceptor.
See [Direct TLS](deployment/tls.md#mutual-tls-client-certificates) for ownership and client-certificate
conditions.

## Reload changed the wrong thing

**Symptom:** `kill -HUP` loads changed Python code but a changed TOML,
environment file, or certificate still has the old value.

**Cause:** SIGHUP reloads application code only. The supervisor keeps its
resolved configuration, inherited environment, listener descriptors, and
prepared TLS acceptor while it re-imports the application in replacement
workers.

**Confirm:** Compare the startup banner and [`--print-config`](configuration.md#command-print_config) output before and
after HUP. Check the worker import log and the certificate presented by a new
connection. If only Python code changed, worker PIDs should roll one at a time;
if a config file changed, the old resolved values remain until restart.

**Fix:** Use HUP only for app-only rolling reload. Validate a new TOML with
[`--check-config`](configuration.md#command-check_config), atomically replace certificate files when applicable, and
restart the supervisor for config, environment, env-file, listener, limit, or
TLS changes. The development [`--reload`](configuration.md#command-reload) watcher is separate from the production
supervisor reload.

## `uvloop` is missing

**Symptom:** Startup rejects [`--loop`](configuration.md#option-loop) `uvloop` with an error naming the
`h2corn[uvloop]` extra.

**Cause:** `uvloop` is optional and is not installed in the base wheel. PyPy,
Windows, and some free-threaded combinations lack a usable uvloop package.

**Confirm:** Run `python -c 'import uvloop; print(uvloop.__version__)'` under
the same interpreter as the `h2corn` executable, and check `h2corn` [`--version`](configuration.md#command-version).

**Fix:** Install `h2corn[uvloop]` on a supported Unix CPython environment, or
choose [`--loop`](configuration.md#option-loop) `asyncio`. `auto` selects uvloop when installed and otherwise
uses the standard-library loop; h2corn's network I/O remains in Rust either
way.

## systemd notify does not become ready

**Symptom:** A `Type=notify` unit stays `activating`, or systemd reports a
stale readiness state during reload.

**Cause:** h2corn sends `READY=1` only after every worker is serving and sends
`STOPPING=1` when shutdown begins. A shell wrapper may hide the main PID, the
unit may not set `NotifyAccess=main`, or [`--reload`](configuration.md#command-reload) may put the watcher between
systemd and the supervisor. `--reload` is incompatible with `Type=notify`.

**Confirm:** Inspect the unit and journal:

```bash
systemctl show myapp.service -p Type -p NotifyAccess -p MainPID -p SubState
journalctl -u myapp.service -b
```

Check that the service uses `Type=notify`, `NotifyAccess=main`, and an exec
form `ExecStart` that leaves h2corn as the main process. Confirm all workers
reach readiness and that `NOTIFY_SOCKET` is present in the service context.

**Fix:** Use the full unit in [Operations](deployment/operations.md#full-systemd-unit),
remove `--reload` from a notify service, and use `systemctl reload` only for
the documented app-only HUP. Set `TimeoutStartSec` and `TimeoutStopSec` from
the application startup and combined shutdown budgets; notifications report state
and leave both timers unchanged.
