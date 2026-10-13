mod frame;
mod location;
mod metric;
mod topology;
use std::fmt;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

pub(crate) use frame::FrameDependentCaches;
use frame::{GeographicTopologyKey, GeographicTopologyProducts};
use metric::MetricPlan;
use topology::TopologyPlan;

pub(crate) use super::metric::{
    ChartBox, ChartEdgeDifferential, ChartEdgeDifferentialBounds, ChartMetric,
    ChartPointDistanceSample, DwithinOutcome, GeodesicMetric, GeodesicNumericalPolicy,
    IndexedSegment, PointEdgeMinimum, PointEdgeQuery, PointEdgeQueryResult,
};
use crate::HeapSize;
use crate::crs::ChartEdge;
use crate::error::Result;
use crate::geographic::{GeodeticPole, GeographicFrame, ScalarGeographicView};
use crate::geometry::antimeridian::validate_structure;
use crate::geometry::distance::ChartParts;
use crate::geometry::interval::Interval;
use crate::geometry::point_location::{PointQueryPlan, PreparedPointQueries};
use crate::geometry::source_support::{
    OccupiedToward, PointMembership, SourcePointLocation, cartesian_point_location_and_area,
    exact_stored_point, occupied_toward,
};
use crate::geometry::tessellation::exact::ExactDyadic;
use crate::geometry::topology::ExactGeographicTopology;
pub(crate) use crate::geometry::types::point_key::{PointKey, canonical_f64_bits};
pub(crate) use crate::geometry::types::results::{PolygonizeFull, ValidationIssue};
pub(crate) use crate::geometry::types::shape_data::{IntoSharedShape, ShapeData};
use crate::geometry::{
    Bounds, ChartLineIndex, DistanceParts, GeographicAreaDensity, GeographicMetricKey,
    GeographicPrepared, GeographicTopologyRoute, LineIndex, LineIndexSlot, MeasuredLineIndex,
    MeasuredLineIndexSlot, PlanarMetric, Point, Segment, Shape, XY,
};
use crate::work::Work;

/// One reusable execution plan for one immutable geometry.
///
/// Persistent prepared geometries, array rows, and spatial-index rows retain
/// this same type. Ordinary one-shot calls construct a transient plan and drop
/// it after the operation. Geodesic products remain in the caller-owned
/// [`FrameDependentCaches`] sidecar because their interpretation depends on
/// ellipsoid and PROJ runtime generation.
pub(crate) struct PreparedPlan {
    shape: Arc<ShapeData>,
    topology: OnceLock<Box<TopologyPlan>>,
    metric: OnceLock<Box<MetricPlan>>,
}

impl PreparedPlan {
    pub(crate) fn new(shape: impl IntoSharedShape) -> Self {
        Self {
            shape: shape.into_shared(),
            topology: OnceLock::new(),
            metric: OnceLock::new(),
        }
    }

    pub(crate) fn transient(shape: &Shape) -> Self {
        Self::new(ShapeData::new(shape.clone()))
    }

    pub(crate) fn shape_data(&self) -> &ShapeData {
        &self.shape
    }

    pub(crate) const fn shape_arc(&self) -> &Arc<ShapeData> {
        &self.shape
    }

    fn topology(&self) -> &TopologyPlan {
        self.topology
            .get_or_init(|| Box::new(TopologyPlan::default()))
            .as_ref()
    }

    fn metric(&self) -> &MetricPlan {
        self.metric
            .get_or_init(|| Box::new(MetricPlan::default()))
            .as_ref()
    }
}
