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
    _unpickle_geometry, _unpickle_geometry_array, _unpickle_line_array, _unpickle_point_array,
    _unpickle_polygon_array, f64_column_le_bytes, usize_row_map_le_bytes,
};
