# New Python reference passed to a borrowed-pointer constructor

CPython's raw pointer type does not distinguish a borrowed reference from a newly
owned reference. PyO3's borrowed-pointer constructors acquire another reference;
they do not consume ownership of the pointer passed to them. Passing a newly
returned reference directly to one of these constructors creates an additional
reference obligation. The binding's `assume_borrowed*` helpers instead create
non-owning views, which also leave the original ownership obligation untouched.
Unless separately released or transferred, the original reference leaks.

## Recommendation

Use the matching owned-pointer constructor to transfer the returned reference
into `Bound`. Preserve the API's null and error handling: for example,
`PyNumber_Index` belongs with `Bound::from_owned_ptr_or_err`. Adjust any later
manual release of the original reference along with the constructor.

The shared contract library identifies reviewed CPython factories and both
public pointer constructors and internal FFI helpers by resolved
declaration, including imported aliases and compatibility implementations.
The query follows exact pointer flow through intermediate variables and helpers.
Owning pointer constructors terminate the unmanaged reference's flow. An
explicit release of that pointer makes its ownership balance ambiguous
and is left for review. Callback effects and general reference balance are
outside this query's guarantee.
