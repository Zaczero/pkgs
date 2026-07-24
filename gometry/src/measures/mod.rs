//! Free-function geometry operations exposed to Python — measures (area,
//! length, ...), constructive ops (buffer, simplify, hulls, triangulation,
//! Voronoi, ...), linear referencing, and CRS set/transform. Thin `PyO3`
//! entry points over the geometry kernel + the crate-root broadcast core.

mod linework;
mod metrics;
mod point_nav;

pub(crate) use linework::*;
pub(crate) use metrics::*;
pub(crate) use point_nav::*;
