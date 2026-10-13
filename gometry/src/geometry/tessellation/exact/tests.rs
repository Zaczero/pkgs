use super::dyadic::ExactOracle as _;
use super::frame::{enclosing_frame, enclosing_frame_binary64};
use super::incidence::{
    angle_ccw_cmp, cycle_orientation, point_in_cycle, segment_intersection, signed_line_product,
    squared_distance_cmp_point_exact,
};
use super::point::verify_interval;
use super::support_circle::{round_support_circle, try_certify_support};
use super::*;
use crate::error::ErrorKind;
include!("tests/support.rs");

include!("tests/arithmetic.rs");
include!("tests/point.rs");
include!("tests/incidence.rs");
include!("tests/frame.rs");
include!("tests/support_circle.rs");
