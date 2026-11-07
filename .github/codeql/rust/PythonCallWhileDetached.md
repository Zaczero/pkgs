# Python object API called while detached

Raw CPython object operations require an attached thread. Passing a raw pointer
or an unsafe wrapper through `Python::detach` does not retain permission to use
the interpreter. This remains true on free-threaded Python.

## Recommendation

Perform Python work before detachment, or attach inside a callback before
accessing Python objects. Carry only native data and appropriately owned
handles through detached work.

The query follows resolved calls from the closure supplied to PyO3's
detachment operation. It does not enter separately supplied callback closures,
so an explicitly attached callback retains its independent context. Dynamically
dispatched and unresolved calls remain analysis gaps.
