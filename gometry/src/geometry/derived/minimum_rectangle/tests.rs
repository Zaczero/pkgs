use super::*;
use crate::geometry::Coordinates as _;

mod support;
use support::{chart_hull, hull_vertices, point, point_selection_chart};

use super::calipers::*;
use super::chart::calipers::*;
use super::chart::corners::*;
use super::chart::publication::*;
use super::corners::*;
use super::score::*;

include!("tests/calipers.rs");
include!("tests/repair.rs");
include!("tests/chart.rs");
