use crate::DistanceUnit;
use crate::dispatch::metric::MetricResolver;
use crate::geometry::OverlayOp;
use crate::predicates::engine::Predicate;

macro_rules! define_operation {
    (
        $(
            $unit:ident => {
                name: $name:literal,
                resolver: $resolver:expr
                $(,)?
            }
        ),* $(,)?
    ) => {
        /// Non-metric operations routed through the shared scalar/array
        /// dispatch spine. Metric operations are carried by [`MetricOp`].
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        pub(crate) enum Operation {
            $($unit,)*
            Metric(MetricOp),
            Predicate(Predicate),
        }

        impl Operation {
            pub(crate) const fn name(self) -> &'static str {
                match self {
                    $(Self::$unit => $name,)*
                    Self::Metric(op) => op.name(),
                    Self::Predicate(predicate) => predicate.spec().token,
                }
            }

            pub(crate) const fn resolver(self) -> MetricResolver {
                match self {
                    $(Self::$unit => $resolver,)*
                    Self::Metric(op) => op.resolver(),
                    Self::Predicate(_) => MetricResolver::None,
                }
            }

            pub(crate) const fn metric_op(self) -> Option<MetricOp> {
                match self {
                    $(Self::$unit => None,)*
                    Self::Metric(op) => Some(op),
                    Self::Predicate(_) => None,
                }
            }
        }
    };
}

/// Metric operations share one resolver boundary but retain their distinct
/// public names. Non-metric operations stay in [`Operation`] so this enum does
/// not become a second registry for unrelated dispatch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum MetricOp {
    Area,
    Bearing,
    Buffer,
    ConcaveHull,
    CrossTrackDistance,
    Destination,
    Distance,
    Distance3d,
    Dwithin,
    FrechetDistance,
    HausdorffDistance,
    InterpolateM,
    Length,
    Length3d,
    LineInterpolate,
    LineLocate,
    LineSubstring,
    MaximumInscribedCircle,
    MaximumInscribedRadius,
    MinimumBoundingCircle,
    MinimumBoundingRadius,
    MinimumClearance,
    MinimumClearanceLine,
    Nearest,
    NearestPoints,
    OffsetCurve,
    PointBetween,
    PointBetweenRhumb,
    Polylabel,
    RhumbBearing,
    RhumbDestination,
    RhumbDistance,
    Segmentize,
    ShortestLine,
    SpatialIndexDistanceQuery,
}

impl MetricOp {
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Area => "area",
            Self::Bearing => "bearing",
            Self::Buffer => "buffer",
            Self::ConcaveHull => "concave_hull",
            Self::CrossTrackDistance => "cross_track_distance",
            Self::Destination => "destination",
            Self::Distance => "distance",
            Self::Distance3d => "distance_3d",
            Self::Dwithin => "dwithin",
            Self::FrechetDistance => "frechet_distance",
            Self::HausdorffDistance => "hausdorff_distance",
            Self::InterpolateM => "interpolate_m",
            Self::Length => "length",
            Self::Length3d => "length_3d",
            Self::LineInterpolate => "line_interpolate",
            Self::LineLocate => "line_locate",
            Self::LineSubstring => "line_substring",
            Self::MaximumInscribedCircle => "maximum_inscribed_circle",
            Self::MaximumInscribedRadius => "maximum_inscribed_radius",
            Self::MinimumBoundingCircle => "minimum_bounding_circle",
            Self::MinimumBoundingRadius => "minimum_bounding_radius",
            Self::MinimumClearance => "minimum_clearance",
            Self::MinimumClearanceLine => "minimum_clearance_line",
            Self::Nearest => "nearest",
            Self::NearestPoints => "nearest_points",
            Self::OffsetCurve => "offset_curve",
            Self::PointBetween => "point_between",
            Self::PointBetweenRhumb => "point_between(path='rhumb')",
            Self::Polylabel => "polylabel",
            Self::RhumbBearing => "bearing(path='rhumb')",
            Self::RhumbDestination => "destination(path='rhumb')",
            Self::RhumbDistance => "rhumb_distance",
            Self::Segmentize => "segmentize",
            Self::ShortestLine => "shortest_line",
            Self::SpatialIndexDistanceQuery => "spatial index distance query",
        }
    }

    #[expect(
        clippy::wildcard_enum_match_arm,
        reason = "ordinary metric operations share this hot resolver; only 3D and linear-reference metrics are specialized above"
    )]
    pub(crate) const fn resolver(self) -> MetricResolver {
        match self {
            Self::Distance3d | Self::Length3d => MetricResolver::Metric3d {
                op: self,
                unit: None,
            },
            Self::LineInterpolate | Self::LineLocate | Self::LineSubstring => {
                MetricResolver::LineMetric {
                    op: self,
                    unit: None,
                    normalized: false,
                }
            },
            _ => MetricResolver::Metric {
                op: self,
                unit: None,
            },
        }
    }
}

define_operation! {
    AffineTransform => { name: "affine_transform", resolver: MetricResolver::None },
    Boundary => { name: "boundary", resolver: MetricResolver::None },
    Bounds => { name: "bounds", resolver: MetricResolver::None },
    BuildArea => { name: "build_area", resolver: MetricResolver::None },
    Centroid => { name: "centroid", resolver: MetricResolver::None },
    ClipByRect => { name: "clip_by_rect", resolver: MetricResolver::None },
    ConvexHull => { name: "convex_hull", resolver: MetricResolver::None },
    CrossesAntimeridian => { name: "crosses_antimeridian", resolver: MetricResolver::Antimeridian },
    Difference => { name: "difference", resolver: MetricResolver::None },
    Envelope => { name: "envelope", resolver: MetricResolver::None },
    Force2d => { name: "force_2d", resolver: MetricResolver::None },
    Force3d => { name: "force_3d", resolver: MetricResolver::None },
    Intersection => { name: "intersection", resolver: MetricResolver::None },
    IsCcw => { name: "is_ccw", resolver: MetricResolver::None },
    IsClosed => { name: "is_closed", resolver: MetricResolver::None },
    IsConvex => { name: "is_convex", resolver: MetricResolver::None },
    IsEmpty => { name: "is_empty", resolver: MetricResolver::None },
    IsRing => { name: "is_ring", resolver: MetricResolver::None },
    IsSimple => { name: "is_simple", resolver: MetricResolver::None },
    IsValid => { name: "is_valid", resolver: MetricResolver::None },
    MinimumRotatedRectangle => { name: "minimum_rotated_rectangle", resolver: MetricResolver::None },
    Node => { name: "node", resolver: MetricResolver::None },
    Normalize => { name: "normalize", resolver: MetricResolver::None },
    OrientPolygons => { name: "orient_polygons", resolver: MetricResolver::None },
    PointOnSurface => { name: "point_on_surface", resolver: MetricResolver::None },
    Quantize => { name: "quantize", resolver: MetricResolver::None },
    RemoveRepeatedPoints => { name: "remove_repeated_points", resolver: MetricResolver::None },
    Repair => { name: "repair", resolver: MetricResolver::None },
    Reverse => { name: "reverse", resolver: MetricResolver::None },
    Rotate => { name: "rotate", resolver: MetricResolver::None },
    Scale => { name: "scale", resolver: MetricResolver::None },
    Skew => { name: "skew", resolver: MetricResolver::None },
    Snap => { name: "snap", resolver: MetricResolver::None },
    SetM => { name: "set_m", resolver: MetricResolver::None },
    SetZ => { name: "set_z", resolver: MetricResolver::None },
    Simplify => { name: "simplify", resolver: MetricResolver::None },
    Smooth => { name: "smooth", resolver: MetricResolver::None },
    SnapToGrid => { name: "snap_to_grid", resolver: MetricResolver::None },
    SplitAntimeridian => { name: "split_antimeridian", resolver: MetricResolver::None },
    SwapXy => { name: "swap_xy", resolver: MetricResolver::None },
    SymmetricDifference => { name: "symmetric_difference", resolver: MetricResolver::None },
    Translate => { name: "translate", resolver: MetricResolver::None },
    Union => { name: "union", resolver: MetricResolver::None },
    UniquePoints => { name: "unique_points", resolver: MetricResolver::None },
}

impl Operation {
    /// Call-site ``unit=`` for CRS-aware ops (merged into [`MetricResolver`]).
    pub(crate) const fn resolver_with_unit(self, unit: Option<DistanceUnit>) -> MetricResolver {
        self.resolver().with_unit(unit)
    }

    pub(crate) const fn resolver_with_line_unit(
        self,
        unit: Option<DistanceUnit>,
        normalized: bool,
    ) -> MetricResolver {
        self.resolver().with_line_unit(unit, normalized)
    }

    pub(crate) const fn from_overlay(op: OverlayOp) -> Self {
        match op {
            OverlayOp::Intersection => Self::Intersection,
            OverlayOp::Union => Self::Union,
            OverlayOp::Difference => Self::Difference,
            OverlayOp::SymmetricDifference => Self::SymmetricDifference,
        }
    }
}
