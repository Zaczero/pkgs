#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::metric::MetricResolver;
use crate::DistanceUnit;
use crate::geometry::OverlayOp;
use crate::py::functions::predicate::Predicate;

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
        /// Operations routed through the shared scalar/array dispatch spine.
        /// Each row owns only the execution data dispatch consumes: its public
        /// name and metric resolver.
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        pub(crate) enum Operation {
            $($unit,)*
            Predicate(Predicate),
        }

        impl Operation {
            pub(crate) fn name(self) -> &'static str {
                match self {
                    $(Self::$unit => $name,)*
                    Self::Predicate(predicate) => predicate.spec().token,
                }
            }

            pub(crate) const fn resolver(self) -> MetricResolver {
                match self {
                    $(Self::$unit => $resolver,)*
                    Self::Predicate(_) => MetricResolver::None,
                }
            }
        }
    };
}

define_operation! {
    AffineTransform => { name: "affine_transform", resolver: MetricResolver::None },
    Area => { name: "area", resolver: MetricResolver::Metric { unit: None } },
    Boundary => { name: "boundary", resolver: MetricResolver::None },
    Bounds => { name: "bounds", resolver: MetricResolver::None },
    Buffer => { name: "buffer", resolver: MetricResolver::Metric { unit: None } },
    BuildArea => { name: "build_area", resolver: MetricResolver::None },
    Centroid => { name: "centroid", resolver: MetricResolver::None },
    ClipByRect => { name: "clip_by_rect", resolver: MetricResolver::None },
    ConcaveHull => { name: "concave_hull", resolver: MetricResolver::Metric { unit: None } },
    ConvexHull => { name: "convex_hull", resolver: MetricResolver::None },
    CrossesAntimeridian => { name: "crosses_antimeridian", resolver: MetricResolver::Antimeridian },
    Difference => { name: "difference", resolver: MetricResolver::None },
    Distance => { name: "distance", resolver: MetricResolver::Metric { unit: None } },
    Distance3d => { name: "distance_3d", resolver: MetricResolver::None },
    Dwithin => { name: "dwithin", resolver: MetricResolver::Metric { unit: None } },
    Envelope => { name: "envelope", resolver: MetricResolver::None },
    Force2d => { name: "force_2d", resolver: MetricResolver::None },
    Force3d => { name: "force_3d", resolver: MetricResolver::None },
    FrechetDistance => { name: "frechet_distance", resolver: MetricResolver::Metric { unit: None } },
    HausdorffDistance => { name: "hausdorff_distance", resolver: MetricResolver::Metric { unit: None } },
    InterpolateM => { name: "interpolate_m", resolver: MetricResolver::Metric { unit: None } },
    Intersection => { name: "intersection", resolver: MetricResolver::None },
    IsCcw => { name: "is_ccw", resolver: MetricResolver::None },
    IsClosed => { name: "is_closed", resolver: MetricResolver::None },
    IsConvex => { name: "is_convex", resolver: MetricResolver::None },
    IsEmpty => { name: "is_empty", resolver: MetricResolver::None },
    IsRing => { name: "is_ring", resolver: MetricResolver::None },
    IsSimple => { name: "is_simple", resolver: MetricResolver::None },
    IsValid => { name: "is_valid", resolver: MetricResolver::None },
    Length => { name: "length", resolver: MetricResolver::Metric { unit: None } },
    Length3d => { name: "length_3d", resolver: MetricResolver::None },
    LineInterpolate => { name: "line_interpolate", resolver: MetricResolver::LineMetric { unit: None, normalized: false } },
    LineLocate => { name: "line_locate", resolver: MetricResolver::LineMetric { unit: None, normalized: false } },
    LineSubstring => { name: "line_substring", resolver: MetricResolver::LineMetric { unit: None, normalized: false } },
    MaximumInscribedCircle => { name: "maximum_inscribed_circle", resolver: MetricResolver::Metric { unit: None } },
    MaximumInscribedRadius => { name: "maximum_inscribed_radius", resolver: MetricResolver::Metric { unit: None } },
    MinimumBoundingCircle => { name: "minimum_bounding_circle", resolver: MetricResolver::Metric { unit: None } },
    MinimumBoundingRadius => { name: "minimum_bounding_radius", resolver: MetricResolver::Metric { unit: None } },
    MinimumClearance => { name: "minimum_clearance", resolver: MetricResolver::Metric { unit: None } },
    MinimumClearanceLine => { name: "minimum_clearance_line", resolver: MetricResolver::Metric { unit: None } },
    MinimumRotatedRectangle => { name: "minimum_rotated_rectangle", resolver: MetricResolver::None },
    Node => { name: "node", resolver: MetricResolver::None },
    Normalize => { name: "normalize", resolver: MetricResolver::None },
    OffsetCurve => { name: "offset_curve", resolver: MetricResolver::Metric { unit: None } },
    OrientPolygons => { name: "orient_polygons", resolver: MetricResolver::None },
    PointOnSurface => { name: "point_on_surface", resolver: MetricResolver::None },
    Polylabel => { name: "polylabel", resolver: MetricResolver::Metric { unit: None } },
    Quantize => { name: "quantize", resolver: MetricResolver::None },
    RemoveRepeatedPoints => { name: "remove_repeated_points", resolver: MetricResolver::None },
    Repair => { name: "repair", resolver: MetricResolver::None },
    Reverse => { name: "reverse", resolver: MetricResolver::None },
    Rotate => { name: "rotate", resolver: MetricResolver::None },
    Scale => { name: "scale", resolver: MetricResolver::None },
    Segmentize => { name: "segmentize", resolver: MetricResolver::None },
    Skew => { name: "skew", resolver: MetricResolver::None },
    Snap => { name: "snap", resolver: MetricResolver::None },
    SetM => { name: "set_m", resolver: MetricResolver::None },
    SetZ => { name: "set_z", resolver: MetricResolver::None },
    Simplify => { name: "simplify", resolver: MetricResolver::None },
    Smooth => { name: "smooth", resolver: MetricResolver::None },
    SnapToGrid => { name: "snap_to_grid", resolver: MetricResolver::None },
    SplitAntimeridian => { name: "split_antimeridian", resolver: MetricResolver::Antimeridian },
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
