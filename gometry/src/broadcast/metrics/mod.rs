#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! CRS-aware metric broadcasts: planar/geodesic distance, dwithin, and the
//! point operations (bearing/destination/interpolate) that depend on the CRS.

use super::*;

crate::tokens::token_enum! {
    /// Distance/area unit override for a CRS-aware metric operation's
    /// `unit` keyword. The keyword is `None` by default (the pythonic
    /// "derive it" spelling): the CRS drives the metric — geodesic meters
    /// on a geographic CRS, native units on a projected one, raw coordinate
    /// units without a CRS. `'planar'` always measures raw coordinate
    /// units; `'meters'` always measures the CRS metric and errors on a
    /// CRS-free geometry, which has no meter scale. Parsed once at the
    /// `PyO3` boundary so the choice flows inward as a `Copy` enum.
    pub enum DistanceUnit("unit", param = "unit") {
        Planar = "planar",
        Meters = "meters",
    }
}
crate::tokens::token_from_pyobject!(DistanceUnit);

mod arrays;
mod pair;
mod points;
mod similarity;

pub(crate) use arrays::*;
pub(crate) use pair::*;
pub(crate) use points::*;
pub(crate) use similarity::*;
