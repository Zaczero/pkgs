//! CRS-aware metric broadcasts: planar/geodesic distance, dwithin, and the
//! point operations (bearing/destination/interpolate) that depend on the CRS.

#[cfg(test)]
use crate::broadcast::ShapeData;
use crate::broadcast::{
    Arc, Bound, Bounds, CollectRows, CoordinateAxes, F64Param, Frame, GeometryArrayStorage,
    GeometryInput, GeometryKind, Point, Py, PyGeometry, PyGeometryArray, PyResult, Python, Result,
    Shape, bool_array, crs, float64_array, mask_missing, point_distance, rows_err,
};

crate::tokens::token_enum! {
    /// Distance/area unit override for a CRS-aware metric operation's
    /// `unit` keyword. The keyword is `None` by default (the pythonic
    /// "derive it" spelling): the CRS drives the metric — ellipsoidal units
    /// for geographic shape measures, native linear units on a projected CRS,
    /// raw coordinate units without a CRS. Geographic shape area and length
    /// follow straight longitude/latitude edges; point distances use geodesics.
    /// `'planar'` uses Cartesian coordinate space (native angular units on a
    /// geographic CRS); `'meters'` always measures the CRS metric and errors on
    /// a CRS-free geometry, which has no meter scale. Parsed once at the
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
    array_crs_distances, array_crs_dwithin, array_crs_dwithin_per_element, array_crs_metric_float,
};
pub(crate) use pair::{
    LonLatPointsError, ensure_geodesic_shape, geodesic_point_columns_dwithin_shape_values,
    geodesic_point_columns_to_shape_values, lonlat_points, lonlat_shape,
    metric_frechet_densified_with_scratch, metric_maximum_inscribed_radius,
    metric_minimum_clearance, metric_minimum_clearance_line, metric_nearest_points_resolved,
    metric_shortest_line_resolved, pair_distance_geodesic, pair_distance_planar,
    pair_dwithin_geodesic, pair_dwithin_planar, resolve_metric, resolve_metric_3d,
    resolve_metric_at, same_storage_similarity_metric_zeros, with_source_arc_context,
};
pub(crate) use similarity::{SimilarityOp, array_crs_frechet_metric_per_densify};
