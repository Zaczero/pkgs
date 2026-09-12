#![allow(
    clippy::needless_pass_by_value,
    clippy::too_many_arguments,
    clippy::trivially_copy_pass_by_ref,
    reason = "PyO3 extractors and method signatures intentionally follow Python ownership and call shapes"
)]

mod methods;

#[expect(
    clippy::rc_buffer,
    clippy::missing_const_for_fn,
    clippy::needless_late_init,
    reason = "Arrow buffers and schema plans share Arc-backed owners and fallible builders across the active import-lane rewrite; changing these representations or signatures is a coordinated Arrow change, not an isolated lint fix"
)]
pub(crate) mod arrow;
#[expect(
    clippy::rc_buffer,
    clippy::ignored_unit_patterns,
    clippy::match_same_arms,
    clippy::elidable_lifetime_names,
    clippy::unnecessary_wraps,
    clippy::manual_let_else,
    clippy::single_match_else,
    clippy::inconsistent_struct_constructor,
    clippy::redundant_clone,
    reason = "Arrow C buffers and schema plans share Arc-backed owners and fallible builders across the active import-lane rewrite; changing these representations or signatures is a coordinated Arrow change, not an isolated lint fix"
)]
pub(crate) mod arrow_c;
pub(crate) mod buffer;
pub(crate) mod cells;
pub(crate) mod classes;
pub(crate) mod crs;
pub(crate) mod errors;
pub(crate) mod functions;
pub(crate) mod geoarrow;
pub(crate) mod index;
pub(crate) mod numpy;
pub(crate) mod replace;
pub(crate) mod row;
pub(crate) mod support;
/// Test-owned PEP-3118 producers (private `_indirect_*` surface).
pub(crate) mod test_buffers;
pub(crate) mod vectors;
pub(crate) mod viz;
pub(crate) mod wire_crs;
