//! Compile-fail fragment: production [`ImmutableI64Owner`] requires `Sealed`.
//!
//! Path-includes the real `frozen_i64_bound.rs` (the module that owns the
//! storage half of the production bound for `numpy::frozen_i64_view`). A type
//! that is not sealed must not be able to implement `ImmutableI64Owner`.
//!
//! Expected: E0277 — the trait bound `Mutable: Sealed` is not satisfied
//! (required for `Mutable` to implement `ImmutableI64Owner`).
//!
//! Mutation proof (mandatory): drop only the `Sealed` supertrait from
//! production `ImmutableI64Owner` → this fixture compiles (green) →
//! `compile_fail_fixtures_guard_production` nextest goes RED. Revert.
//!
//! NOTE: production `frozen_i64_view` *also* requires
//! `T: PyClass<Frozen = boolean_struct::True>` (see `numpy.rs`). That
//! language-level bound cannot be path-included under the bare-`rustc`
//! compile-fail harness — `#[pyclass]` needs the PyO3 proc-macro stack and
//! full dependency graph, which this harness deliberately does not link. A
//! genuine mutable `#[pyclass]` negative is therefore inexpressible here
//! (STOP-AND-REPORT for the fixture half only). The Frozen half is enforced
//! by the production signature + cargo typecheck; this fixture keeps the
//! sealed storage half under the existing gate.

#[path = "../../frozen_i64_bound.rs"]
mod frozen_i64_bound;

use frozen_i64_bound::{ImmutableI64Owner, frozen_i64_owner, require_immutable_i64_owner};

/// Proven owner — implements the production seal (same shape as `Groups`).
struct FrozenGroups;
impl frozen_i64_owner::Sealed for FrozenGroups {}
impl ImmutableI64Owner for FrozenGroups {}

/// Mutable stand-in: deliberately does **not** implement `Sealed`.
///
/// A genuine mutable `#[pyclass]` cannot be expressed under bare-rustc
/// (see module docs); this plain struct still proves the Sealed supertrait
/// is load-bearing for `ImmutableI64Owner`.
struct Mutable;

// ERROR: the trait bound `Mutable: Sealed` is not satisfied
// (required for `Mutable` to implement `ImmutableI64Owner`).
// This is the property under test — not a missing `impl` of the marker itself.
impl ImmutableI64Owner for Mutable {}

fn main() {
    // Happy path still type-checks against the production gate function.
    require_immutable_i64_owner(&FrozenGroups);
    // Once Sealed is dropped from ImmutableI64Owner, this impl succeeds and
    // Mutable is admitted — fixture goes green under that exact regression.
    require_immutable_i64_owner(&Mutable);
}
