use super::*;
use crate::geographic::{AngularFactor, FullTurnPole, ScalarGeographicView};
use crate::geometry::overlay::{OverlayOp, Strictness, overlay_all};
use crate::geometry::topology::overlay::exact_geographic_overlay;
use crate::geometry::{
    CoordSeq, CoordinateAxes, LineSeq, MultiMember, MultiPolygon, Polygon, Ring,
};
mod support;
use support::*;

pub(super) mod subgraph_oracle;

include!("tests/collapsed_support.rs");

include!("tests/cut_selection.rs");

include!("tests/global_faces.rs");

include!("tests/location.rs");

include!("tests/occupancy.rs");

include!("tests/rectangle.rs");

include!("tests/rotation.rs");

include!("tests/spatial_index.rs");
