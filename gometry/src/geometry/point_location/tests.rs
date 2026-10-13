use super::*;
use crate::geometry::overlay::{AreaLocationStrategy, ExactSourceInventory};
use crate::geometry::source_support::{
    OccupiedToward, cartesian_exact_offset_location, cartesian_exact_offset_location_and_area,
    cartesian_exact_point_location, cartesian_exact_point_location_and_area,
};
use crate::geometry::{CoordWindow, LineSeq, MultiPolygon, ring_classify_point, same_point};

include!("tests/support.rs");
include!("tests/ring.rs");
include!("tests/polygon.rs");
include!("tests/batch.rs");
