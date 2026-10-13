use std::cell::Cell;
use std::cmp::Ordering;

use super::query::AxisRelation;
use super::rays::{
    curve_has_point, for_each_ring_segment, for_each_xy_segment, formal_ring_winding, ray_groups,
    ring_has_point, ring_kind, ring_probe_columns,
};
use super::*;

include!("tests/reference.rs");
include!("tests/support.rs");
include!("tests/query.rs");
include!("tests/streaming.rs");
include!("tests/rays.rs");
