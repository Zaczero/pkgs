//! Free-function geometry operations exposed to Python — measures (area,
//! length, ...), constructive ops (buffer, simplify, hulls, triangulation,
//! Voronoi, ...), linear referencing, and CRS set/transform. Thin `PyO3`
//! entry points over the geometry kernel + the crate-root broadcast core.

mod linework;
mod metrics;
mod point_nav;

pub(crate) use linework::{shared_paths, split};
pub(crate) use metrics::{
    area, area_natural_array, area_natural_scalar, bounds, bounds_array, length, length_3d,
    length_natural_array, length_natural_scalar, snap,
};
pub(crate) use point_nav::{
    bearing, cross_track_distance, destination, destination_point_receiver, point_between,
    reject_rhumb_unit, rhumb_destination_point_receiver, rhumb_distance,
};
