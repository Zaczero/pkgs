#![allow(
    clippy::needless_pass_by_value,
    clippy::too_many_arguments,
    clippy::trivially_copy_pass_by_ref,
    reason = "PyO3 extractors and method signatures intentionally follow Python ownership and call shapes"
)]

mod methods;

pub(crate) mod arrow;
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
pub(crate) mod vectors;
pub(crate) mod viz;
