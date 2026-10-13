use crate::crs::ChartEdge;
use crate::error::Result;
use crate::geographic::{GeodeticPole, GeographicFrame};
use crate::geometry::interval::Interval;
use crate::geometry::{GeographicAreaDensity, Point, Segment, Shape};
use crate::work::Work;

/// Numerical quality budgets shared by geographic point-edge and Hausdorff searches.
#[derive(Clone, Copy, Debug)]
pub(crate) struct GeodesicNumericalPolicy {
    semi_major: f64,
}

impl GeodesicNumericalPolicy {
    pub(crate) const BACKEND_RELATIVE: f64 = 1e-11;
    pub(crate) const MINIMUM_GAP_RELATIVE: f64 = 4e-11;
    pub(crate) const HAUSDORFF_GAP_RELATIVE: f64 = 0.01 / 6_378_137.0;

    pub(crate) const fn new(semi_major: f64) -> Self {
        Self { semi_major }
    }

    pub(crate) fn backend_floor(self) -> f64 {
        Interval::exact(self.semi_major)
            .mul(Interval::exact(Self::BACKEND_RELATIVE))
            .upper()
    }

    pub(crate) fn minimum_gap(self) -> f64 {
        self.semi_major * Self::MINIMUM_GAP_RELATIVE
    }

    pub(crate) fn hausdorff_gap(self) -> f64 {
        self.semi_major * Self::HAUSDORFF_GAP_RELATIVE
    }

    pub(crate) fn evaluation_allowance(self, evaluations: f64) -> f64 {
        Interval::exact(self.semi_major)
            .mul(Interval::exact(Self::BACKEND_RELATIVE))
            .mul(Interval::exact(evaluations.max(1.0)))
            .upper()
    }
}

/// Ellipsoidal point-to-point distance primitives. Chart-edge operations use
/// [`ChartMetric`] and the same ellipsoid adapter.
pub(crate) trait GeodesicMetric {
    /// Runtime generation used to namespace CRS-dependent prepared state.
    /// Geometry-only test metrics use the default zero generation.
    fn runtime_generation(&self) -> u64 {
        0
    }

    /// Admit a shape in the metric's coordinate frame before any geodesic
    /// working set is built. Geometry-only metrics leave admission to their
    /// caller; CRS-backed metrics validate their native angular axes here.
    fn ensure_shape(&self, _shape: &Shape) -> Result<()> {
        Ok(())
    }

    /// Geodesic length (meters) of segment `a`–`b`.
    fn segment_length(&self, a: Point, b: Point) -> f64;

    /// A cheap lower bound on the geodesic distance between two points.
    /// Preparation failures propagate instead of disabling the bound.
    fn point_distance_lower_bound(&self, a: Point, b: Point) -> Result<f64>;

    /// Shared numerical policy for this ellipsoid.
    fn geodesic_numerical_policy(&self) -> GeodesicNumericalPolicy;
}

/// Metric operations on the straight edges of an admitted coordinate chart.
///
/// The edge representation belongs to the metric provider; geometry code
/// remains independent of the CRS and ellipsoid implementation.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ChartBox {
    /// Canonical Greenwich degrees. `west > east` denotes a seam-crossing
    /// interval; `[-180, 180]` denotes the full longitude range.
    pub(crate) west: f64,
    pub(crate) south: f64,
    pub(crate) east: f64,
    pub(crate) north: f64,
}

crate::heapless!(ChartBox);

#[derive(Clone, Copy, Debug)]
pub(crate) enum PointEdgeQuery {
    Minimum { cutoff: f64 },
    Nearest { cutoff: f64 },
    Locate { cutoff: f64 },
    Dwithin { limit: f64 },
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct PointEdgeMinimum {
    pub(crate) distance: f64,
    pub(crate) parameter: f64,
    pub(crate) lower_bound: f64,
    pub(crate) evaluations: usize,
    pub(crate) budget_exhausted: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DwithinOutcome {
    Within,
    Outside,
    Unresolved,
}

impl DwithinOutcome {
    pub(crate) const fn combine(self, other: Self) -> Self {
        match (self, other) {
            (Self::Within, _) | (_, Self::Within) => Self::Within,
            (Self::Unresolved, _) | (_, Self::Unresolved) => Self::Unresolved,
            (Self::Outside, Self::Outside) => Self::Outside,
        }
    }

    pub(crate) fn into_bool(self) -> Result<bool> {
        match self {
            Self::Within => Ok(true),
            Self::Outside => Ok(false),
            Self::Unresolved => Err(crate::crs::CrsError::message(
                "geodesic dwithin could not certify the threshold within its evaluation work limit",
            )),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ChartEdgeDifferential {
    pub(crate) speed: f64,
    pub(crate) azimuth: f64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ChartEdgeDifferentialBounds {
    pub(crate) speed: f64,
    pub(crate) covariant_acceleration: f64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ChartPointDistanceSample {
    pub(crate) distance: f64,
    pub(crate) derivative: f64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum PointEdgeQueryResult {
    Minimum(PointEdgeMinimum),
    Excluded,
    Within {
        evaluations: usize,
    },
    Outside {
        lower_bound: f64,
        evaluations: usize,
    },
    Unresolved {
        lower_bound: f64,
        evaluations: usize,
    },
}

pub(crate) trait ChartMetric {
    type PointEdgeScratch: Default;

    fn chart_frame(&self) -> GeographicFrame;
    fn chart_edge(&self, start: Point, end: Point) -> Result<ChartEdge>;
    fn chart_edge_length(&self, edge: &ChartEdge) -> Result<f64>;
    fn chart_edge_length_with_work<W: Work>(&self, edge: &ChartEdge, work: &mut W) -> Result<f64>;
    fn chart_edge_length_to(&self, edge: &ChartEdge, parameter: f64) -> Result<f64>;
    fn chart_edge_arclength_fraction(
        &self,
        edge: &ChartEdge,
        parameter: f64,
        total_length: Option<f64>,
    ) -> Result<f64>;
    fn chart_edge_arclength_fraction_with_work<W: Work>(
        &self,
        edge: &ChartEdge,
        parameter: f64,
        total_length: Option<f64>,
        work: &mut W,
    ) -> Result<f64>;
    fn chart_edge_locate_t(&self, edge: &ChartEdge, distance: f64, length: f64) -> Result<f64>;
    /// Measure a source edge interval without rounding exact chart-cut points.
    fn chart_edge_interval_length(&self, edge: &ChartEdge, start: f64, end: f64) -> Result<f64>;
    fn chart_edge_witness(
        &self,
        start: Point,
        end: Point,
        edge: &ChartEdge,
        parameter: f64,
        length: Option<f64>,
    ) -> Result<Point>;
    fn chart_edge_differential(&self, edge: &ChartEdge, parameter: f64) -> ChartEdgeDifferential;
    fn chart_edge_differential_bounds(&self, edge: &ChartEdge) -> ChartEdgeDifferentialBounds;
    fn chart_edge_moving_correspondence_upper(
        &self,
        source: &ChartEdge,
        target: &ChartEdge,
        source_interval: [f64; 2],
        target_interval: [f64; 2],
        source_points: &[Point; 2],
        target_points: &[Point; 2],
        endpoint_distances: [f64; 2],
    ) -> Option<f64>;
    fn chart_edge_pole_distance_upper_bound(
        &self,
        edge: &ChartEdge,
        source_interval: [f64; 2],
        target: Point,
    ) -> Option<f64>;
    fn point_edge_query(
        &self,
        point: Point,
        edge: &ChartEdge,
        envelope_lower: f64,
        query: PointEdgeQuery,
        scratch: &mut Self::PointEdgeScratch,
    ) -> Result<PointEdgeQueryResult>;
    fn chart_point_distance_sample(
        &self,
        source: Point,
        target: Point,
        source_speed: f64,
        source_azimuth: f64,
    ) -> Result<ChartPointDistanceSample>;
    fn geodesic_distance_hessian_upper_bound(
        &self,
        lower_distance: f64,
        upper_distance: f64,
    ) -> f64;
    fn point_envelope_bound(&self, point: Point) -> impl Fn(ChartBox) -> f64;
    fn chart_area_density(
        &self,
        reference_colatitude: f64,
        reference_pole: GeodeticPole,
    ) -> GeographicAreaDensity;
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct IndexedSegment {
    pub segment: Segment,
    pub line: usize,
    pub index: usize,
    pub count: usize,
    pub closed: bool,
}

#[cfg(test)]
mod tests;
