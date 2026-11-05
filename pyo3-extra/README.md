# pyo3-extra

Internal CPython primitives shared by the monorepo's PyO3 extensions. The crate
uses PyO3's objects, conversions, ownership and exception handling directly.

Object construction, borrowed views, reference ownership and interpreter API
selection belong to the PyO3 fork. This crate composes those primitives into
literal caching, macros and protocol helpers; it does not duplicate their C
declarations or object-layout rules.

## Dictionary construction

```rust
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_extra::dict;

fn message<'py>(
    py: Python<'py>,
    body: &Bound<'py, PyAny>,
    more_body: bool,
) -> PyResult<Bound<'py, PyDict>> {
    Ok(dict!(py, {
        "type" => "http.response.body",
        "body" => body,
        if more_body => {
            "more_body" => true,
        },
    }))
}
```

`dict!` reuses literal keys and values and accepts ordinary `IntoPyObject`
expressions. Unconditional fields use PyO3's borrowed bulk constructor, allowing
CPython to select compact string-key storage without Python pair objects.
Conditional fields are inserted incrementally. Borrowed Python values stay
borrowed during construction. Expressions run once; omitted
conditional fields do not evaluate their values. Conversion errors release
temporary construction state and propagate through the caller's `PyResult`.

Use PyO3's `intern!` directly for individual key lookups. `cached_string!` and
`cached_bytes!` select from a fixed vocabulary with an uncached fallback. Their
caches contain only declared literals, never request values or dynamic schemas.
These static caches follow PyO3's single-interpreter extension model.

## Protocols and buffers

- `iter` distinguishes absent protocols from exceptions raised by providers,
  preserves one-shot iteration, and supplies signal-aware collectors. Lengths
  reserve storage; only an explicit exact count or maximum bounds consumption.
- `protocol` admits and invokes Python's integer-index protocol without replacing
  provider exceptions with application-specific errors.
- `buffer` acquires PyO3 buffer handles, checks dimensions and stride arithmetic,
  and captures one-byte buffers. `with_bytes` borrows immutable Python bytes and
  copies other exporters before invoking its callback. Exporters must remain
  quiescent during capture; acquisition alone does not synchronize their writes.
- `buffer::export` owns the metadata and release mechanics for contiguous,
  read-only typed exports. Its unsafe contract requires stable, immutable
  owner-backed storage for the entire export lifetime.
- `types` supplies sequence-pattern initialization and concise module registration.

Domain-specific validation, error messages and conversion policies belong to
the consuming package.
