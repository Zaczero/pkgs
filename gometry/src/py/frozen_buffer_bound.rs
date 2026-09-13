//! Bound for the owner-derived NumPy views in [`crate::py::numpy`].
//!
//! Intentionally **std-only** so compile-fail fixtures can `#[path]`-include
//! this production module (same doctrine as `arrow_c/foreign_buffer.rs`). The
//! full view helpers additionally require PyO3's
//! `T: PyClass<Frozen = True>` bound at the call site; that language-level
//! bound cannot live in this std-only module.
//!
//! The seal is the storage half of the view safety claim: only an owner whose
//! retained backing is immutable may opt in. The higher-ranked closure below
//! makes the returned slice borrow from that same owner rather than accepting
//! an independently supplied owner and slice.

/// Seal for [`ImmutableBufferOwner`].
pub(crate) mod frozen_buffer_owner {
    /// Sealed marker — not implementable outside the crate without the private
    /// trait in this module.
    pub(crate) trait Sealed {}
}

/// Marker for a frozen owner whose retained storage is immutable.
///
/// The production view helpers also require `PyClass<Frozen = True>` (see
/// `numpy.rs`). The `Sealed` supertrait is load-bearing: without it, any type
/// could opt into the storage half of the safety claim.
pub(crate) trait ImmutableBufferOwner: frozen_buffer_owner::Sealed {}

/// Production gate used by the owner-derived NumPy view helpers.
///
/// Keeping this call at each helper couples its bound to this module so the
/// storage seal cannot silently drift. Compile-fail fixtures path-include this
/// file and prove that the seal and owner-borrowing contract remain required.
pub(crate) const fn require_immutable_buffer_owner<T: ImmutableBufferOwner>(_owner: &T) {}

/// Borrow a slice derived from the owner for exactly the owner's lifetime.
///
/// The higher-ranked closure cannot return a borrow of a temporary or a
/// separately owned local value. The production helpers use this before
/// handing the slice to rust-numpy's `borrow_from_array`.
pub(crate) fn owner_derived_slice<'owner, Owner, Value>(
    owner: &'owner Owner,
    pick: impl for<'a> FnOnce(&'a Owner) -> &'a [Value],
) -> &'owner [Value]
where
    Owner: ImmutableBufferOwner,
{
    pick(owner)
}
