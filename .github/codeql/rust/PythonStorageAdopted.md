# Python-owned object storage adopted as a Rust allocation

Pointers returned by Python bytes, bytearray and Unicode data accessors borrow
the object's storage. They do not transfer an allocation to the caller.
Constructing a Rust `Box`, `Vec`, `String`, `Arc` or `Rc` from such a pointer can
free Python-owned memory through the wrong allocator or ownership protocol.

## Recommendation

Retain the Python owner while using a bounded borrowed view, or copy the data
into a separately owned Rust allocation. Do not use a raw-parts ownership
constructor as a way to obtain a borrowed slice.

The query follows local pointer flow from reviewed object data accessors to
global-allocator ownership constructors. Capsules are deliberately separate:
their payload may use a legitimate explicit ownership-transfer protocol.
