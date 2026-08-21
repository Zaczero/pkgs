---
description: User-visible changes and upgrade actions for h2corn releases.
---

# Release notes

## h2corn 1.6.0

Forwarded-header interpretation is now selected by `forwarded_fields`.
The default is `for,proto`, which interprets `X-Forwarded-For` and
`X-Forwarded-Proto` from a trusted proxy. `host`, `port`, and `prefix` opt into
their corresponding X-Forwarded facts. `forwarded` opts into RFC 7239
`Forwarded` and cannot be combined with an X-Forwarded value.

When `proxy_headers` is enabled, recognized forwarding headers that h2corn does
not consume are removed from the ASGI header list. This includes underscore
spellings accepted by the HTTP field-name grammar. Applications that relied on
the previous automatic interpretation of host, port, prefix, or `Forwarded`
must set `forwarded_fields` to the required dialect and facts. So must an
application that reads one of those headers itself rather than through the
scope — Django under `USE_X_FORWARDED_HOST`, or a proxy middleware in the
stack — because an unconsumed header no longer reaches it either.

`proxy_headers` with an empty `forwarded_fields` or an empty
`forwarded_allow_ips` is now rejected at construction. Either describes a trust
boundary that can never act, which is what `proxy_headers=False` spells.
