use super::{
    PoleClosure, certified_no_chart_events, chart_line, chart_polygon, chart_ring,
    line_has_certified_no_chart_events, split_polygon,
};
use crate::geographic::{
    AngularFactor, ChartPole, GeodeticPole, GeographicAxisOrientation, GeographicFrame,
    source_admission_counts,
};
use crate::geometry::antimeridian::segment_coords_with_metric;
use crate::geometry::ordinates::{
    SourceArcRequest, source_arc_ordinates, take_source_arc_observations,
};
use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational};
use crate::geometry::{
    ChartMetric as _, CoordSeq, CoordinateAxes, Coordinates as _, GeometryErrorKind, LineSeq,
    MOrdinate, MultiLineString, MultiMember, MultiPolygon, Point, Polygon, Ring, RootMetricState,
    Shape, Unmetered, ZOrdinate,
};
use crate::test_support::allocation_count;

include!("tests/support.rs");
include!("tests/chart_events.rs");
include!("tests/chart_shape.rs");
include!("tests/assemble.rs");
include!("tests/hole_overlay.rs");
include!("tests/chart_ring.rs");
