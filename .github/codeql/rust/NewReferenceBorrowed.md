# New Python reference passed to a borrowed-pointer constructor

CPython's raw pointer type does not distinguish a borrowed reference from a newly
owned reference. PyO3's borrowed-pointer constructors acquire another reference;
they do not consume ownership of the pointer passed to them. Passing a newly
returned reference directly to one of these constructors creates an additional
reference obligation. Unless separately released, the original reference leaks.

## Recommendation

Use the matching owned-pointer constructor to transfer the returned reference
into `Bound`. Preserve the API's null and error handling: for example,
`PyNumber_Index` belongs with `Bound::from_owned_ptr_or_err`. Adjust any later
manual release of the original reference along with the constructor.

The query checks direct conversions from the resolved CPython APIs
`PyNumber_Index`, `PyBytes_FromStringAndSize`, `PyList_New`, `PyTuple_New`,
`PyCapsule_New` and `Py_NewRef`, including imported aliases. It does not track
reference ownership through intermediate variables or helper functions.
