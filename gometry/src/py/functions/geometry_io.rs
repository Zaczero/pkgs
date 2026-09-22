//! Geometry deserialization free functions: WKT, WKB, and `GeoJSON` input.
//!
//! `from_wkt`/`from_wkb`/`from_geojson` parse single geometries or sequences
//! into `Geometry`/`GeometryArray`, sharing the crate-root codecs and array
//! packing via `use super::*`. (Serialization lives on the `#[pymethods]`.)

mod feature;
mod from;
mod geojson;
mod pickle;

pub(crate) use feature::{to_feature, to_feature_collection};
pub(crate) use from::{from_wkb, from_wkt};
pub(crate) use geojson::{from_features, from_geojson};
pub(crate) use pickle::{
    _unpickle_geometry_array_v1, _unpickle_geometry_v1, _unpickle_packed_array_v1, reduce_geometry,
    reduce_geometry_array,
};
