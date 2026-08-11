//! Bound for [`crate::py::numpy::frozen_i64_view`].
//!
//! Intentionally **std-only** so compile-fail fixtures can `#[path]`-include
//! this production module (same doctrine as `arrow_c/foreign_buffer.rs`). The
//! full `frozen_i64_view` needs PyO3 and additionally requires
//! `T: PyClass<Frozen = True>` at the call site — that language-level frozen
//! bound cannot live in this std-only module.
//!
//! The **seal** here is the second half of the safety claim: even among frozen
//! pyclasses, only opt-in immutable Arc i64 owners may take the view.
//! [`ImmutableI64Owner`]'s [`frozen_i64_owner::Sealed`] supertrait blocks a
//! random type from implementing the marker.
//!
//! Production `frozen_i64_view` requires both bounds and calls
//! [`require_immutable_i64_owner`] so the seal half cannot silently drift.
//! Compile-fail `item_c` proves: a type that does not implement `Sealed`
//! cannot implement `ImmutableI64Owner`. Dropping only the `Sealed` supertrait
//! makes that fixture green (nextest red).

/// Seal for [`ImmutableI64Owner`]: only frozen pyclasses with immutable Arc
/// i64 backing may opt in. A mutable `PyClass` is already rejected by the
/// `PyClass<Frozen = True>` bound on `frozen_i64_view`; this seal additionally
/// blocks a frozen-but-mutable-backing type from forging the marker.
pub(crate) mod frozen_i64_owner {
    /// Sealed marker — not implementable outside the crate without the private
    /// trait in this module.
    pub(crate) trait Sealed {}
}

/// Marker: the owner proves frozen immutable Arc-backed i64 storage.
///
/// Production `frozen_i64_view` also requires `PyClass<Frozen = True>` (see
/// `numpy.rs`). The `Sealed` supertrait is load-bearing for the storage half:
/// without it, any type could implement this marker.
pub(crate) trait ImmutableI64Owner: frozen_i64_owner::Sealed {}

/// Production gate used by [`crate::py::numpy::frozen_i64_view`].
///
/// Forces the `ImmutableI64Owner` bound at the call site so the where-clause
/// and this module stay coupled. Compile-fail fixtures path-include this file
/// and prove `Sealed` is required to implement `ImmutableI64Owner`.
pub(crate) const fn require_immutable_i64_owner<T: ImmutableI64Owner>(_owner: &T) {}
