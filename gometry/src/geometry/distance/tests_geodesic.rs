use geographiclib_rs::{Geodesic, InverseGeodesic as _};
use pyo3::prelude::*;

use crate::crs::frame::{Frame, crs_arc, crs_arc_static};
use crate::crs::{PointEdgeScratch, point_edge_test_counters, with_ellipsoid_metric};
use crate::error::Result;
use crate::geographic::{AngularFactor, GeographicFrame};
use crate::geometry::distance::frechet::frechet_dp_columns;
use crate::geometry::distance::tests::{line, p, polygon};
use crate::geometry::distance::{
    ChartHausdorffWitness, ChartParts, chart_directed_hausdorff_distance, chart_hausdorff_counters,
    chart_parts_counters, chart_scratch,
};
use crate::geometry::{
    ChartMetric, FrameDependentCaches, GeodesicMetric, GeographicPrepared, MOrdinate, Point,
    PointEdgeQueryResult, PreparedPlan, Shape, ZOrdinate, graph_relate_data_geographic,
};

const MINIMUM_GAP_METERS: f64 = 4e-11 * 6_378_137.0;

mod distance;
mod dwithin;
mod frechet;
mod hausdorff;
mod support;
