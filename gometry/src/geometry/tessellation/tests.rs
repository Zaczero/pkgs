use super::test_support::{
    assert_constrained_triangulation_error, assert_triangulation_error, polygon, xy, xym, xyz,
};
use crate::error::ErrorKind;
use crate::geographic::retained_exact::sites::retained_site_longitude;
use crate::geographic::{GeographicFrame, PointSelectionChart};
use crate::geometry::numerics::product::ExactSign;
use crate::geometry::segments::{Orientation, stored_orientation_sign};
use crate::geometry::tessellation::delaunay::exact_delaunay::MeshCoordinates as _;
use crate::geometry::tessellation::delaunay::{
    cleanup_constraint_lines, take_constrained_output_reserves, take_delaunay_output_reserves,
};
use crate::geometry::tessellation::earcut::{
    cleaned_earcut_ring, open_ring, validate_earcut_ring, validate_hole_interactions,
};
use crate::geometry::tessellation::shape::{
    BudgetedPackedTriangleSink, BudgetedTriangleSink, canonical_voronoi_sites,
};
use crate::geometry::{
    CdtRefinement, CoordSeq, CoordinateAxes, Coordinates as _, ExpansionBudget,
    GENERATED_ITEM_LIMIT, GeometryErrorKind, LineSeq, MOrdinate, MultiMember, MultiPolygon, Point,
    Polygon, Shape, Strictness, TopologyDomain, XY, ZOrdinate, open_point_cycle_magnitude,
    orientation, ring_contains_interior,
};

include!("tests/exact_incidence.rs");
include!("tests/voronoi_snap.rs");
include!("tests/public_triangulation.rs");
include!("tests/constrained_triangulation.rs");
include!("tests/retained_mesh.rs");
include!("tests/earcut.rs");
