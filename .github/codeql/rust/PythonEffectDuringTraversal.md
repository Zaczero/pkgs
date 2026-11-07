# Python execution or reference-count effect during GC traversal

GC traversal describes an object's existing strong reference edges. It must
not allocate Python objects, invoke Python callbacks, acquire attachment, or
change the reference counts it is reporting.

## Recommendation

Use `PyVisit` to report each owned Python edge. Move mutation, conversion and
reference destruction to ordinary execution or the object's clear operation.
Clear must publish its valid cleared state before dropping references that
can reenter Python.

The query follows resolved calls from a traversal method taking a `PyVisit`
parameter to reviewed Python effects. It does not prove that every edge is
visited, that shared ownership is counted correctly, or that traversal locks
are deadlock-free.
