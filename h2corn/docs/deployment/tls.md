---
description: Terminate TLS in h2corn itself with Rustls, including mutual TLS and reading the client identity from an app.
---

# Direct TLS

`h2corn` can terminate TLS itself instead of relying on a reverse proxy.
This fits single-server deployments, sidecar-fronted services, or any
environment where running an extra edge process isn't worthwhile.

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
and TLS 1.3 only**. ALPN advertises `h2,http/1.1` by default, or only
`h2` when `--no-http1` is set. OpenSSL cipher strings, legacy TLS
versions, and encrypted private-key files are intentionally not
supported — pre-decrypt your key files with operator tooling instead of
shipping a passphrase to a long-running server.

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

| `--cert-reqs` | Behavior                                                     |
| ------------- | ------------------------------------------------------------ |
| `none`        | Do not request client certificates. Default.                 |
| `optional`    | Request a client certificate and verify it if presented.     |
| `required`    | Reject the handshake when no valid client certificate is presented. |

When `--cert-reqs` is anything other than `none`, `--ca-certs` is
required, and the listener must already have a server certificate and
key configured.

## Reading the connection from an application

Verification decides whether a client gets in. To decide what it may
*do*, the application needs to know who it is — so h2corn implements the
[ASGI TLS extension](https://asgi.readthedocs.io/en/latest/specs/tls.html),
which puts the negotiated parameters under `scope["extensions"]["tls"]`:

```python
async def app(scope, receive, send):
    tls = scope['extensions'].get('tls')
    if tls is None:
        ...  # not a TLS connection h2corn terminated
    chain = tls['client_cert_chain']       # PEM strings, leaf first
    name = tls['client_cert_name']         # RFC 4514 leaf DN, or None
    version = tls['tls_version']           # 0x0304 for TLS 1.3
    suite = tls['cipher_suite']            # e.g. 0x1302
```

The key is present only on connections h2corn terminated itself. Behind
a TLS-terminating proxy there is no `tls` key at all, which is what the
extension requires — the proxy holds that information, and forwards
what it chooses to as headers.

`client_cert_chain` is empty unless `--cert-reqs` asked for a
certificate. When it is not empty, every certificate in it was verified
against `--ca-certs` during the handshake, so an application may trust
the identity without re-checking it, and `client_cert_name` is the leaf
subject rendered once as an RFC 4514 string (including §2.4 `#`+hex
encoding for dotted-decimal AttributeTypes). An empty optional-auth
chain leaves both the tuple empty and the name `None`.
`client_cert_error` is always `None`: a certificate that fails
verification fails the handshake and never reaches an application.

The dictionary is built once per connection, so every request on a keep-alive
or multiplexed HTTP/2 connection is handed the same object.

Certificate and key files are read once, while the process is still
privileged, and every worker reuses what the supervisor read. No worker
reopens a PEM path after dropping privileges or when it is replaced, so the
key may stay `root:root` mode `0600`.

## Restrictions

- Direct TLS is only supported on TCP listeners. A configuration that
  combines TLS with a `unix:` listener is rejected at startup.
- TLS 1.0/1.1 and SSLv3 are not configurable.
- Encrypted private-key files (passphrase-protected PEM) are not
  supported.

If you need anything outside these constraints — uncommon ciphers,
client-cert revocation lists, or SNI multiplexing — terminate TLS at a
dedicated proxy and run `h2corn` on `h2c` upstream as described in
[Behind a proxy](proxy.md).
