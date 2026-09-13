//! Compile-fail fragment: an owner-derived NumPy view cannot borrow a temporary.
//!
//! The higher-ranked production closure must return a slice derived from its
//! owner argument. Returning a local vector would leave the NumPy view without
//! a live owner.
//!
//! Expected: E0515 — cannot return reference to local variable `values`.

#[path = "../../frozen_i64_bound.rs"]
mod frozen_i64_bound;

use frozen_i64_bound::{ImmutableI64Owner, frozen_i64_owner, owner_derived_i64_slice};

struct FrozenGroups;
impl frozen_i64_owner::Sealed for FrozenGroups {}
impl ImmutableI64Owner for FrozenGroups {}

fn main() {
    let owner = FrozenGroups;
    let _: &[i64] = owner_derived_i64_slice(&owner, |_| {
        let values = vec![1_i64, 2_i64];
        // ERROR: the returned slice cannot outlive this temporary vector.
        &values
    });
}
