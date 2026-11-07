# Borrowed Python reference passed to a stealing API

A stealing CPython API consumes an existing strong reference to its argument.
Passing a borrowed result or a PyO3 owner's `as_ptr()` consumes a reference
which still belongs to that provider. Its eventual release can then destroy an
object that the receiving container still uses.

## Recommendation

Transfer a separately owned reference with `into_ptr()`, or use the binding's
safe container method. Acquiring an additional reference before transfer is
also valid.

The shared contract identifies the consumed argument by resolved declaration.
It distinguishes always-consuming APIs from `PyModule_AddObject`, whose
failure path retains the caller's ownership. Both require an owned argument
on a successful call. The rule follows local pointer aliases; explicit local
increfs are left for ownership-balance review.
