# Application data sent through an address-selection probe

HTTPX Secure may open a TCP probe to select an endpoint before authorizing it.
That probe is permitted to inspect endpoint metadata and close the stream. It
must not send application bytes or start TLS: the selected address has not yet
passed the policy.

## Recommendation

Keep the selection probe limited to endpoint metadata and cancellation-safe
closure. Send application data only through the protected backend after
admission of the selected address.

The query identifies AnyIO probe creation in the owned address-policy resolver
and follows the resulting stream through aliases and helper functions to
data-writing or TLS operations. Other application connections are not probes.
