use std::sync::Arc;

use geographiclib_rs::Geodesic;

use super::super::GeodesicMetric;
use super::ChartEdge;
use super::arclength::ONE_DEGREE;
use super::authalic::AuthalicSeries;
use crate::crs::geodesic::geographic_metric_frame_at;
use crate::geographic::retained_exact::edges::retained_source_interval_initial_piece_count;
use crate::geographic::retained_exact::sites::retained_source_sheet_site;
use crate::geographic::{
    AngularFactor, ChartStep, EdgeDirection, GeographicAxisOrientation, GeographicFrame,
};
use crate::geometry::{
    ChartMetric as _, GeodesicMetric as _, GeometryErrorKind, MOrdinate, Point, ZOrdinate,
};
use crate::work::Work;

mod support;

use support::{degrees, edge_length, grads, radians, south_axis_degrees, wgs84};

include!("tests/arclength.rs");
include!("tests/bounds.rs");
include!("tests/construction.rs");
include!("tests/derivatives.rs");
include!("tests/metric.rs");
include!("tests/source_interval.rs");
