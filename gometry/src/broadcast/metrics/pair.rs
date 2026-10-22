use crate::array::PyGeometryArray;
use crate::broadcast::metrics::{
    CoordinateAxes, DistanceUnit, GeometryArrayStorage, GeometryKind, Point, PyGeometry, PyResult,
    Shape, crs,
};
use crate::crs::frame::Frame;
use crate::crs::local::{LocalInputPurpose, PreparedLocalInput, geodesic_by_identity};
use crate::dispatch::MetricOp;
use crate::geographic::{ComputedEdgeIntent, GeographicFrame, lonlat as geographic};
use crate::geometry::storage::packed_columns::PackedColumns;
use crate::geometry::storage::{PackedStorage, PointRows};
use crate::geometry::{
    FrameDependentCachePair, FrameDependentCaches, LineSeq, PreparedPlan, ShapeCountQuery,
    SourceArcCapture, SourceArcContext, TopologyDomain, storage,
};
use crate::{NonNegative, validate_lonlat_shape};

mod clearance;
mod distance;
mod points;
mod resolve;
mod similarity;

pub(crate) use clearance::{metric_minimum_clearance, metric_minimum_clearance_line};
pub(crate) use distance::{
    metric_nearest_points_resolved, metric_shortest_line_resolved, pair_distance_geodesic,
    pair_distance_planar, pair_dwithin_geodesic, pair_dwithin_planar,
};
pub(crate) use points::{
    LonLatPointsError, geodesic_point_columns_dwithin_shape_values,
    geodesic_point_columns_to_shape_values, lonlat_points,
};
use resolve::{dispatch_metric_model, dispatch_resolved_metric, prepare_geodesic_pair};
pub(crate) use resolve::{
    ensure_geodesic_shape, ensure_hausdorff_geodesic_shape, lonlat_shape, resolve_metric,
    resolve_metric_3d, resolve_metric_at, with_source_arc_context,
};
pub(crate) use similarity::{
    metric_frechet_densified_with_scratch, metric_hausdorff, same_storage_similarity_metric_zeros,
};
