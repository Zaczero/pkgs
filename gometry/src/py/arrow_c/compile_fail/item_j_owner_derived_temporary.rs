//! Compile-fail fragment: an owner-derived NumPy view cannot borrow a temporary.
//!
//! The higher-ranked production closure must return a slice derived from its
//! owner argument. Returning a local vector would leave the NumPy view without
//! a live owner.
//!
//! Expected: E0515 — cannot return reference to local variable `values`.

#[path = "../../frozen_buffer_bound.rs"]
mod frozen_buffer_bound;

use frozen_buffer_bound::{ImmutableBufferOwner, frozen_buffer_owner, owner_derived_slice};

struct FrozenGroups;
impl frozen_buffer_owner::Sealed for FrozenGroups {}
impl ImmutableBufferOwner for FrozenGroups {}

fn main() {
    let owner = FrozenGroups;
    let _: &[f64] = owner_derived_slice(&owner, |_| {
        let values = vec![1.0, 2.0];
        // ERROR: the returned slice cannot outlive this temporary vector.
        &values
    });
}
