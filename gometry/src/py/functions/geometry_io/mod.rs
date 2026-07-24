//! Geometry deserialization free functions: WKT, WKB, and `GeoJSON` input.
//!
//! `from_wkt`/`from_wkb`/`from_geojson` parse single geometries or sequences
//! into `Geometry`/`GeometryArray`, sharing the crate-root codecs and array
//! packing via `use super::*`. (Serialization lives on the `#[pymethods]`.)

mod feature;
mod from;
mod pickle;

pub(crate) use feature::*;
pub(crate) use from::*;
pub(crate) use pickle::*;
