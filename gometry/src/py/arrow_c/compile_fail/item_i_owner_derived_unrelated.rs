//! Compile-fail fragment: owner-derived NumPy views reject an unrelated owner.
//!
//! The production helper accepts only a type admitted by the immutable-owner
//! seal. An arbitrary owner cannot be paired with a slice, even when the slice
//! itself has a valid lifetime.
//!
//! Expected: E0277 — `Unrelated` does not implement `ImmutableBufferOwner`.
//! The immutable marker is the storage half of the production view contract;
//! PyO3's frozen-class half is enforced by the full helper signature.

#[path = "../../frozen_buffer_bound.rs"]
mod frozen_buffer_bound;

use frozen_buffer_bound::owner_derived_slice;

struct Unrelated;

fn main() {
    let owner = Unrelated;
    // ERROR: an unrelated owner cannot opt into the production view contract.
    let _: &[f64] = owner_derived_slice(&owner, |_| &[]);
}
