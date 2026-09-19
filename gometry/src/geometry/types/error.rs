use thiserror::Error;

use crate::error::Error as CrateError;
use crate::geometry::CoordinateAxes;

#[derive(Debug, Error)]
pub enum GeometryErrorKind {
    #[error("coordinates must be finite")]
    NonFiniteCoordinate,
    #[error("coordinate lanes must have the same length, got x={0} and {1}")]
    CoordinateLength(usize, usize),
    #[error("coordinate axes must match the builder's declared axes, got {declared:?} and {got:?}")]
    CoordinateAxesMismatch {
        declared: CoordinateAxes,
        got: CoordinateAxes,
    },
    #[error(
        "coordinate window {start}..{end} is outside the coordinate sequence of length {length}"
    )]
    CoordinateRange {
        start: usize,
        end: usize,
        length: usize,
    },
    #[error("{param} must be finite, got {value}")]
    NonFinite { param: &'static str, value: f64 },
    #[error("{0} must be a non-negative finite number, got {1}")]
    NonNegativeFinite(&'static str, f64),
    #[error("{0} must be a positive finite number, got {1}")]
    PositiveFinite(&'static str, f64),
    #[error("{message}")]
    Parameter {
        param: &'static str,
        value: f64,
        message: Box<str>,
    },
    #[error("projection failed: {0}")]
    Projection(Box<str>),
    #[error("projection failed: {0}")]
    ProjectionDomain(Box<str>),
    #[error("polygon rings require at least three coordinates, got {0}")]
    RingTooShort(usize),
    #[error("clip rectangle bounds must be finite and ordered min <= max")]
    InvalidRectangle,
    #[error(
        "geographic clip rectangle bounds must use finite longitudes in [-180, 180] and south <= north"
    )]
    InvalidGeographicRectangle,
    #[error("non-empty linework is required")]
    EmptyLinework,
    #[error("is_convex requires a Polygon")]
    SinglePolygonRequired,
    #[error("coverage operations require Polygon or MultiPolygon rows")]
    CoveragePolygonalRequired,
    #[error(
        "{operation} requires a valid polygonal coverage (no overlaps or T-junctions); inspect coverage_invalid_edges(), repair with coverage_clean(), or use union_all()/dissolve() for arbitrary polygons"
    )]
    InvalidCoverage { operation: &'static str },
    #[error("interpolate_m measures must be finite with end_m >= start_m, got {0} and {1}")]
    InvalidMeasureRange(f64, f64),
    #[error(
        "line_substring requires start <= end, got {0} and {1}; use reverse() for a reversed sub-line"
    )]
    SubstringOrder(f64, f64),
    #[error(
        "interpolate_m would overwrite existing M ordinates; pass overwrite=True to replace them"
    )]
    MeasureOverwrite,
    #[error(
        "{operation} would generate {produced} items, exceeding the limit of {limit}; reduce {parameter}"
    )]
    GeneratedOutputTooLarge {
        operation: &'static str,
        parameter: &'static str,
        produced: usize,
        limit: usize,
    },
    #[error("buffer side requires a non-negative distance (the side encodes direction)")]
    NegativeSidedBufferDistance,
    #[error("buffer side requires lineal geometry (LineString or MultiLineString)")]
    SidedBufferRequiresLineal,
    #[error("linear referencing requires a LineString or MultiLineString")]
    LineStringRequired,
    #[error("a LineString or MultiLineString is required")]
    LinealRequired,
    #[error("line_locate requires a Point")]
    LocatePointRequired,
    #[error("split requires a Point or MultiPoint splitter")]
    PointSplitterRequired,
    #[error("a Polygon or MultiPolygon is required")]
    PolygonRequired,
    #[error(
        "snap_to_grid size is too fine for the coordinate magnitude (the snapped coordinate \
         overflowed)"
    )]
    SnapGridTooFine,
    #[error("split_antimeridian failed: {0}")]
    AntimeridianSplitFailed(Box<str>),
    #[error(
        "split_antimeridian cannot preserve Z/M on inferred pole-closure vertices; call \
         set_z(None).set_m(None) first"
    )]
    AntimeridianPoleOrdinates,
    #[error("{0}")]
    InvalidDe9imPattern(Box<str>),
    #[error("triangulation failed: {0}")]
    Triangulation(Box<str>),
    #[error("Voronoi construction failed: {0}")]
    Voronoi(Box<str>),
    #[error("exact ratio has a zero denominator")]
    ExactRatioZeroDenominator,
    #[error("exact vertex is outside the finite binary64 range")]
    ExactCoordinateOutOfRange,
    #[error(
        "exact geometry embedding failed: distinct nodes {first} and {second} round to the same coordinate"
    )]
    ExactNodeCollision { first: u32, second: u32 },
    #[error(
        "exact geometry embedding failed: unprescribed contact between emitted items {first} and {second}"
    )]
    ExactUnexpectedContact { first: usize, second: usize },
    #[error("exact geometry embedding failed: cyclic order changed at node {node}")]
    ExactCyclicOrder { node: u32 },
    #[error("exact geometry embedding failed: global face changed at node {node}")]
    ExactGlobalFace { node: u32 },
    #[error("Frechet distance requires a LineString or single-line MultiLineString")]
    FrechetLineStringRequired,
    #[error("{0} cannot represent Z/M; remove the unsupported ordinates explicitly first")]
    OrdinateDropped(Box<str>),
    #[error("measured linear referencing requires every vertex to carry an M ordinate")]
    MissingMeasure,
    #[error("measured linear referencing requires monotonic non-decreasing M values")]
    NonMonotonicMeasure,
    #[error("3D operation requires a Z ordinate on the input geometry")]
    MissingZ,
    #[error("{operation} requires at least one geometry")]
    EmptyGeometrySequence { operation: &'static str },
    #[error("line string requires at least two vertices (or none)")]
    LineStringTooShort,
    #[error("line string requires at least two finite coordinates")]
    UnrepairableLineString,
    #[error("multi line string has no repairable members")]
    UnrepairableMultiLineString,
    #[error("geometry repair failed: {0}")]
    RepairFailed(Box<str>),
    #[error("packed geometry exceeds the i32 offset capacity for GeoArrow list arrays")]
    OffsetCapacityExceeded,
    #[error("{operation} output could not be represented as a contained geometry")]
    UnrepresentableOutput { operation: &'static str },
    #[error("offset buffer is malformed: CSR offsets must start at 0 and be non-decreasing")]
    MalformedCsrOffsets,
    /// A serialized pickle payload violated the private packed-storage
    /// contract. This is a Python `GeometryError`, not a geometry validity
    /// failure: the payload itself is the malformed value.
    #[error("{0}")]
    PicklePayload(Box<str>),
    #[error("{0}")]
    Invalid(Box<str>),
}

impl GeometryErrorKind {
    #[expect(
        clippy::impl_trait_in_params,
        reason = "the factory accepts any message conversion; a named generic is not part of its error API"
    )]
    pub fn projection(message: impl Into<Box<str>>) -> CrateError {
        Self::Projection(message.into()).into()
    }

    #[expect(
        clippy::impl_trait_in_params,
        reason = "the factory accepts any message conversion; a named generic is not part of its error API"
    )]
    pub fn projection_domain(message: impl Into<Box<str>>) -> CrateError {
        Self::ProjectionDomain(message.into()).into()
    }

    #[expect(
        clippy::impl_trait_in_params,
        reason = "the factory accepts any message conversion; a named generic is not part of its error API"
    )]
    pub fn triangulation(message: impl Into<Box<str>>) -> CrateError {
        Self::Triangulation(message.into()).into()
    }

    #[expect(
        clippy::impl_trait_in_params,
        reason = "the factory accepts any message conversion; a named generic is not part of its error API"
    )]
    pub fn voronoi(message: impl Into<Box<str>>) -> CrateError {
        Self::Voronoi(message.into()).into()
    }

    #[expect(
        clippy::impl_trait_in_params,
        reason = "the factory accepts any message conversion; a named generic is not part of its error API"
    )]
    pub fn repair_failed(message: impl Into<Box<str>>) -> CrateError {
        Self::RepairFailed(message.into()).into()
    }

    #[expect(
        clippy::impl_trait_in_params,
        reason = "the factory accepts any message conversion; a named generic is not part of its error API"
    )]
    pub fn ordinate_dropped(operation: impl Into<Box<str>>) -> CrateError {
        Self::OrdinateDropped(operation.into()).into()
    }

    #[expect(
        clippy::impl_trait_in_params,
        reason = "the factory accepts any message conversion; a named generic is not part of its error API"
    )]
    pub fn antimeridian_split_failed(message: impl Into<Box<str>>) -> CrateError {
        Self::AntimeridianSplitFailed(message.into()).into()
    }

    #[expect(
        clippy::impl_trait_in_params,
        reason = "the factory accepts any message conversion; a named generic is not part of its error API"
    )]
    pub fn invalid_de9im_pattern(message: impl Into<Box<str>>) -> CrateError {
        Self::InvalidDe9imPattern(message.into()).into()
    }

    pub fn finite(name: &'static str, value: f64) -> CrateError {
        Self::NonFinite { param: name, value }.into()
    }

    pub fn non_negative_finite(name: &'static str, value: f64) -> CrateError {
        Self::NonNegativeFinite(name, value).into()
    }

    pub fn positive_finite(name: &'static str, value: f64) -> CrateError {
        Self::PositiveFinite(name, value).into()
    }

    #[expect(
        clippy::impl_trait_in_params,
        reason = "the factory accepts any message conversion; a named generic is not part of its error API"
    )]
    pub fn parameter(param: &'static str, value: f64, message: impl Into<Box<str>>) -> CrateError {
        Self::Parameter {
            param,
            value,
            message: message.into(),
        }
        .into()
    }

    /// Generic geometry/coordinate value error carrying a verbatim message —
    /// for invalid-input conditions that don't fit a specific variant
    /// (e.g. non-finite geodesic coordinates). Maps to the Python
    /// `InvalidGeometryError`.
    #[expect(
        clippy::impl_trait_in_params,
        reason = "the factory accepts any message conversion; a named generic is not part of its error API"
    )]
    pub fn message(message: impl Into<Box<str>>) -> CrateError {
        Self::Invalid(message.into()).into()
    }

    pub fn unrepresentable_output(operation: &'static str) -> CrateError {
        Self::UnrepresentableOutput { operation }.into()
    }

    #[expect(
        clippy::impl_trait_in_params,
        reason = "the factory accepts any message conversion; a named generic is not part of its error API"
    )]
    pub fn pickle_payload(message: impl Into<Box<str>>) -> CrateError {
        Self::PicklePayload(message.into()).into()
    }
}
