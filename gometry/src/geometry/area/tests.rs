use super::lifted::{exactly_represented_lift, lifted_exact_ring_area_sign, raw_ring_certificate};
use super::polygon::exact_polygon_area_measure_with;
use super::sign::{area_sign_from_ordering, exact_open_point_sign};
use super::*;
include!("tests/support.rs");
include!("tests/lifted.rs");
include!("tests/sign.rs");
include!("tests/shoelace.rs");
include!("tests/polygon.rs");
include!("tests/exact.rs");
include!("tests/reference.rs");

#[cfg(test)]
#[path = "tests/reference/simd.rs"]
mod reference;
