//! Compile-fail fragment: an owner-derived i64 view rejects an unrelated owner.
//!
//! The production helper accepts only a type admitted by the immutable-owner
//! seal. An arbitrary owner cannot be paired with a slice, even when the slice
//! itself has a valid lifetime.
//!
//! Expected: E0277 — `Unrelated` does not implement `ImmutableI64Owner`.
//! The immutable marker is the storage half of the production view contract;
//! PyO3's frozen-class half is enforced by the full helper signature.

#[path = "../../frozen_i64_bound.rs"]
mod frozen_i64_bound;

use frozen_i64_bound::owner_derived_i64_slice;

struct Unrelated;

fn main() {
    let owner = Unrelated;
    // ERROR: an unrelated owner cannot opt into the production view contract.
    let _: &[i64] = owner_derived_i64_slice(&owner, |_| &[]);
}
