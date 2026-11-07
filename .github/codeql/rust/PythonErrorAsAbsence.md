# Python allocation or protocol error interpreted as absence

CPython object-producing APIs commonly return NULL with a pending exception on
failure. A PyO3 optional pointer constructor returns `None` for NULL without
fetching that exception. Interpreting this as ordinary absence both loses the
failure and leaves the interpreter's exception state inconsistent with success.

## Recommendation

Use the matching `*_or_err` constructor for a fallible producer. If the raw
result was already checked, fetch or otherwise handle its exception on the NULL
branch before proceeding with the non-null pointer.

The query distinguishes reviewed failure-on-NULL factories from APIs whose
NULL means ordinary absence. It follows pointer flow and recognizes non-null
guards for the same pointer.
