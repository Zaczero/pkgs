# Consumed Python writer used again

CPython bytes, Unicode and integer writers have one live handle. `Finish`
consumes it even if the result is an error. `Discard` abandons a still-live
writer; it must not clean up a writer which has already finished.

## Recommendation

Use the binding's consuming writer API. At a raw boundary, transfer the handle
exactly once to either `Finish` or `Discard`, and remove any subsequent
operation on it. Finish failure propagates the Python error without a second
release.

The query tracks handles from creation or parameters through local aliases and
`NonNull` pointer views, including handles extracted from `Result` fields of a
`ManuallyDrop` owner. A later operation is reported only when the consuming
call dominates it, so mutually exclusive finish/discard branches are not
mistaken for double release.
Implicit Rust destruction and interprocedural ownership require separate analysis.
