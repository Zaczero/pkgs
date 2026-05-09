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
