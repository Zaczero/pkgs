use super::*;
use crate::geometry::derived;
use crate::geometry::generated_budget::GENERATED_ITEM_LIMIT;
#[cfg(debug_assertions)]
use crate::geometry::point_location::{
    query_comparisons, query_edge_tests, reset_query_comparisons,
};
use crate::geometry::source_support::cartesian_exact_point_location_and_area;

mod support;
use support::{adjacent_box_prepared_support, adjacent_box_union, line, polygon};

use super::cycle::*;
use super::inradius::*;
use super::queue::*;
use super::search::*;
use super::span_bounds::*;
use super::sqrt::*;
use super::support::*;
use super::symmetric::*;
use super::upper::*;

include!("tests/support_cases.rs");
include!("tests/sqrt_cases.rs");
include!("tests/refinement.rs");
include!("tests/inradius_cases.rs");
