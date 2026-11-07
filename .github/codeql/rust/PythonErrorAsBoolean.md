# Python error status interpreted as Boolean true

Several CPython predicates return `1` for true, `0` for false and `-1` with a
pending exception on failure. Testing only whether their result is nonzero
turns an exception into true without handling the error.

## Recommendation

Use the binding's `PyResult<bool>` operation, or branch on the negative error
status before interpreting the remaining value. A non-failing C type check
such as `PyIndex_Check` has a different contract and needs no error branch.

This query follows the result of reviewed fallible predicates to a nonzero
comparison. A matching nonnegative guard constrains that flow only on its
successful branch; a check for a different value is not sufficient.
