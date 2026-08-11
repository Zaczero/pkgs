//! CRS-aware metric broadcasts: planar/geodesic distance, dwithin, and the
//! point operations (bearing/destination/interpolate) that depend on the CRS.

use crate::broadcast::{
    Arc, Bound, Bounds, CollectRows, CoordinateAxes, Crs, EmptyKind, F64Param, Frame,
    GeometryArrayStorage, GeometryError, GeometryInput, Point, Py, PyAnyMethods, PyGeometry,
    PyGeometryArray, PyResult, Python, Result, Shape, ShapeData, bool_array, broadcast2_geometry,
    classify_input, classify_required, crs, float64_array, geometry_type_err, mask_missing,
    paired_arrays, point_distance, rows_err, validate_lonlat_shape,
};

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
mod similarity;

pub(crate) use arrays::{
    array_crs_distances, array_crs_dwithin, array_crs_dwithin_per_element,
    array_crs_dwithin_scalar, array_crs_metric_float, crs_metric_binary_geometry_broadcast,
};
pub(crate) use pair::{
    OptionalDensifyParam, binary_frame_crs, finite_geodesic_value,
    geodesic_point_columns_dwithin_shape_values, geodesic_point_columns_to_shape_values,
    lonlat_shape, lonlat_shape_under, metric_frechet_densified, metric_hausdorff_densified,
    metric_maximum_inscribed_radius, metric_minimum_clearance, metric_minimum_clearance_line,
    metric_nearest_points, metric_shortest_line, pair_distance_resolved,
    pair_distance_resolved_result, pair_dwithin_resolved, pair_dwithin_resolved_result,
    pair_dwithin_shapes, require_point, resolve_metric, resolve_metric_3d,
    same_storage_similarity_metric_zeros, validate_densify,
};
pub(crate) use similarity::{array_crs_similarity_metric_per_densify, crs_aware_dwithin};
