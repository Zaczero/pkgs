# Borrowed Python reference consumed as an owner

A borrowed CPython reference remains owned by its provider. An owning PyO3
constructor consumes an existing strong reference; it does not acquire one.
Adopting a borrowed reference can therefore cause an early release and subsequent
use of a destroyed object.

## Recommendation

Use `Bound::from_borrowed_ptr*` to acquire a strong reference, or explicitly obtain
a new reference before transferring it to an owning constructor. Preserve the
producer's distinction between ordinary absence and a pending Python exception.

The query follows local pointer flow from reviewed borrowed-reference APIs to
resolved owning constructors. Explicit local increfs make ownership ambiguous
and are left for review; this query does not prove release balance across
callbacks or arbitrary helper functions.
