---
description: Terminate TLS in h2corn itself with Rustls, including mutual TLS and reading the client identity from an app.
---

# Direct TLS

`h2corn` can terminate TLS itself instead of relying on a reverse proxy.
This fits single-server deployments, sidecar-fronted services, or any
environment where running an extra edge process isn't worthwhile.

`hello:app` is the import target from the [Quickstart](../quickstart.md).
Replace it and the `/etc/ssl/example/` paths with the application's import
target and certificate files used by the deployment.

Direct TLS is opt-in. Configure a certificate chain and private key
(typically obtained from [Let's Encrypt](https://letsencrypt.org/) or a
similar ACME provider):

```bash
h2corn hello:app \
  --bind 0.0.0.0:8443 \
  --certfile /etc/ssl/example/fullchain.pem \
  --keyfile /etc/ssl/example/privkey.pem
```

`h2corn` uses [Rustls](https://github.com/rustls/rustls) with **TLS 1.2
and TLS 1.3 only**. ALPN advertises `h2,http/1.1` by default, or only `h2` when
[`--no-http1`](../configuration.md#option-http1) is set. OpenSSL cipher strings, legacy TLS versions, and encrypted
private-key files are unsupported. Decrypt key files with operator tooling
before starting the server.

This exposes an HTTPS listener publicly. Restrict access with your firewall as
needed, protect the private key, and arrange certificate renewal.
For edge routing and abuse controls, use a [reverse proxy](proxy.md).

## Mutual TLS (client certificates)

Provide a CA bundle and choose whether client certificates are optional
or required:

```bash
h2corn hello:app \
  --certfile /etc/ssl/example/fullchain.pem \
  --keyfile /etc/ssl/example/privkey.pem \
  --ca-certs /etc/ssl/example/client-ca.pem \
  --cert-reqs required
```

| [`--cert-reqs`](../configuration.md#option-cert_reqs) | Behavior                                                     |
| ------------- | ------------------------------------------------------------ |
| `none`        | No client certificate is requested. Default.                 |
| `optional`    | Request a client certificate and verify it if presented.     |
| `required`    | Reject the handshake when no valid client certificate is presented. |

When [`--cert-reqs`](../configuration.md#option-cert_reqs) is anything other than `none`, [`--ca-certs`](../configuration.md#option-ca_certs) is
required, and the listener must already have a server certificate and
key configured.

## Reading the connection from an application

When a client certificate is presented, its verification decides whether the
handshake succeeds. The [ASGI TLS extension](https://asgi.readthedocs.io/en/latest/specs/tls.html)
exposes the verified identity and negotiated parameters under the
[TLS extension mapping](../api/scopes.md#h2corn.HTTPExtensions.tls):

```python title="partial application fragment"
async def app(scope, receive, send):
    tls = scope['extensions'].get('tls')
    if tls is None:
        return  # not a TLS connection h2corn terminated
    chain = tls['client_cert_chain']       # PEM strings, leaf first
    name = tls['client_cert_name']         # RFC 4514 leaf DN, or None
    version = tls['tls_version']           # 0x0304 for TLS 1.3
    suite = tls['cipher_suite']            # e.g. 0x1302
```

The key is present only on connections h2corn terminated itself. Behind
a TLS-terminating proxy there is no `tls` key at all, which is what the
extension requires — the proxy holds that information, and forwards
what it chooses to as headers.

`client_cert_chain` is empty unless [`--cert-reqs`](../configuration.md#option-cert_reqs) asked for a
certificate. When it is not empty, every certificate in it was verified
against [`--ca-certs`](../configuration.md#option-ca_certs) during the handshake, so an application may trust
the verified certificate identity. `client_cert_name` is the leaf subject as
an RFC 4514 string. With optional authentication and no client certificate,
the chain is empty and the name is `None`.
`client_cert_error` is always `None`: a certificate that fails
verification fails the handshake and never reaches an application.

The [known extension mappings](../api/scopes.md#extension-mapping-fields), including `scope["extensions"]["tls"]`, are
shared per connection and read-only. The outer `scope["extensions"]` mapping is per-scope and
may receive application namespaced keys. Copy a known mapping before mutating
it.

Certificate and key files are read once, while the process is still
privileged, and every worker reuses what the supervisor read. No worker
reopens a PEM path after dropping privileges or when it is replaced, so the
key may stay `root:root` mode `0600`.

## Rotation and restart

The supervisor reads the certificate chain, private key, and client CA bundle
once at startup. It does not watch PEM paths, and `SIGHUP` only reloads the
application.

Rotate a direct-TLS certificate by writing complete files beside
the old ones, setting ownership and permissions, atomically renaming them,
running `h2corn` [`--check-config`](../configuration.md#command-check_config) with the same TLS options, and restarting the
supervisor.

The preflight catches parse and permission errors; a full restart
follows service-manager stop/start behavior and may have downtime. A rolling
application reload preserves the old worker until its replacement is ready.

When a reverse proxy terminates TLS, rotate and validate the certificate there.
h2corn sees h2c, so its [`tls` ASGI extension](../api/scopes.md#h2corn.HTTPExtensions.tls) is absent on that hop.

## Restrictions

Direct TLS is only supported on TCP listeners. A configuration that
combines TLS with a `unix:` listener is rejected at startup.

Uncommon ciphers, client-cert revocation lists, and SNI multiplexing require a
dedicated TLS proxy. Run `h2corn` on `h2c` upstream as described in
[Behind a proxy](proxy.md#behind-a-reverse-proxy).
