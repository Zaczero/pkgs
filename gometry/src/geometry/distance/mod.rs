#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Planar and geodesic distance, `dwithin`, nearest-points, Hausdorff and
//! Fréchet distance on `Shape`, plus the geodesic linear-referencing methods.

use super::*;
use crate::error::Result;

mod distance_3d;
mod frechet;
mod geodesic_hausdorff_helpers;
mod geodesic_parts;
mod geodesic_scratch;
mod geodesic_sweep;
mod hausdorff;
mod intersects;
mod misc;
mod nearest;
mod parts_sweep;
mod shape_data_distance;
mod shape_data_nearest;
mod shape_impl;

pub(crate) use distance_3d::*;
pub(crate) use frechet::*;
pub(crate) use geodesic_hausdorff_helpers::*;
pub(crate) use geodesic_parts::*;
pub(crate) use geodesic_scratch::*;
pub(crate) use geodesic_sweep::*;
pub(crate) use hausdorff::*;
pub(super) use intersects::*;
pub(crate) use misc::*;
pub(super) use nearest::*;
pub(crate) use parts_sweep::*;
#[cfg(test)]
mod hausdorff_stats_test;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_geodesic;
