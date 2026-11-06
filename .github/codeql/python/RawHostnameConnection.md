# Unadmitted hostname reaches a protected connection

HTTPX Secure authorizes a selected numeric IP before opening its data-bearing
connection. Reusing the logical hostname can trigger another DNS resolution and
connect to an address that the policy did not authorize.

This query follows hostname data through aliases, transformations and helper
calls into the protected backend's connection destination. Only results from the
connector's own address-policy provider stop that flow; an unrelated method named
`admit` does not. Plain TCP selection probes are a separate operation and are
permitted by the package's contract.

## Recommendation

Pass the numeric address returned by the address policy to the data-bearing
backend. Do not fall back to the original hostname after admission fails.
Retain the logical origin separately for HTTP authority and TLS verification.

The query checks hostname reuse in the protected connector. It assumes the
address policy honors its admission contract; it does not prove that policy's
implementation or compare authorized and connected ports.
