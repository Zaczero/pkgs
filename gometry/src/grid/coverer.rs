//! Hierarchical coverer shared by the rectangular grids (geohash and XYZ
//! tiles).
//!
//! Descend from the root cells, classify each cell's lon/lat rectangle
//! exactly against the source geometry, and emit the cells at the target
//! depth.
//!
//! Geohash and tile cells are exact axis-aligned lon/lat rectangles (their
//! meridian/parallel edges are straight in lon/lat), so classification is a
//! plain `covers`/`intersects` of the source against the cell box — no
//! spherical-edge ambiguity like S2. The descent prunes empty branches and
//! bulk-emits fully-covered branches, so it never enumerates the whole
//! target grid for a small geometry.

use std::sync::{Arc, OnceLock};

use crate::geometry::{
    Bounds, CoordSeq, Point, PointBatchTester, Polygon, PreparedPlan, RectContact, Segment,
    SegmentIndex, Shape, Strictness, XY, bounds_to_shape, graph_intersects_bounds, line_segments,
    segment_vs_rect,
};
use crate::grid::affine_source::{
    GeographicCoverageSource, RectClass as AffineRectClass, SphericalGridTarget,
};
use crate::grid::spherical_arc::{
    Bound, CertifiedDegreeWindows, CertifiedLongitudeDegrees, DegreeWindowResult,
};
use crate::grid::{CoverError, ensure_cover_budget};
use crate::predicates::engine::{Predicate, PreparedTopologyPair, topology_scalar_pair};

/// A cell of a rectangular lon/lat grid (geohash or tile).
///
/// Contract: `Ord`/`Eq` are cell *identity* — two values compare equal iff they
/// are the same cell — and `edge_neighbors` returns canonically-encoded cells
/// (so a neighbour `binary_search`es equal to the same cell built any other
/// way). `rect_dissolve` relies on both; an implementor that breaks either
/// would silently dissolve the wrong region.
pub(crate) trait RectCell: Copy + Eq + Ord {
    /// This cell's depth (geohash precision / tile zoom).
    fn depth(self) -> u8;
    /// The cell's exact lon/lat bounding rectangle.
    fn bounds(self) -> Bounds;
    /// The children one level finer.
    fn children(self) -> impl ExactSizeIterator<Item = Self>;
    /// Push each child with its lon/lat bounds into `out`, derived from
    /// `parent_bounds` when the grid allows (tiles: four children from parent
    /// edges + one shared latitude split). Default recomputes `child.bounds()`.
    fn push_children_bounds(self, parent_bounds: Bounds, out: &mut Vec<(Self, Bounds)>) {
        let _ = parent_bounds;
        for child in self.children() {
            out.push((child, child.bounds()));
        }
    }
    /// The edge-adjacent neighbours across the four rectangle sides, in the
    /// CCW order `[south(miny), east(maxx), north(maxy), west(minx)]` —
    /// matching the corner order `(minx,miny) -> (maxx,miny) -> (maxx,maxy)
    /// -> (minx,maxy)`. `None` where no lon/lat-adjacent same-depth cell
    /// exists: a grid edge (a pole row), or the antimeridian — which is
    /// deliberately NOT wrapped, so the dissolve emits that side and the
    /// outline splits at ±180 instead of merging the long way around
    /// longitude 0.
    fn edge_neighbors(self) -> [Option<Self>; 4];
}

impl PartialOrd for super::tile::Tile {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for super::tile::Tile {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id().cmp(&other.id())
    }
}

/// Where a cell sits relative to the source geometry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RectClass {
    Outside,
    Interior,
    Boundary,
}

/// Rectangular-grid cell selection policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RectCoverRule {
    Overlap,
    Center,
    Within,
}

/// Native rectangle-vs-geometry classifier shared by the rectangular grids
/// and S2's lon/lat rectangle certificates.
pub(crate) struct NativeRectClassifier {
    source_bounds: Option<Bounds>,
    area: Option<AreaRectClassifier>,
    line_index: Option<SegmentIndex>,
    points: Box<[Point]>,
}

/// Candidate admission is owned by the unsplit periodic source. The native
/// classifier remains the rule-refinement working image; disagreement can
/// only widen to `Boundary`, never turn a chart cut into a negative.
struct GeographicCandidateClassifier<'a> {
    authority: GeographicCoverageSource,
    working: NativeRectClassifier,
    prepared: crate::geometry::GeographicPrepared<'a>,
    authority_graph: OnceLock<Arc<crate::geometry::TopologyGraph>>,
}

impl<'a> GeographicCandidateClassifier<'a> {
    fn new(
        authority: &Shape,
        working: &Shape,
        authority_plan: &'a PreparedPlan,
        authority_cache: &'a crate::geometry::FrameDependentCaches,
        frame: crate::geographic::GeographicFrame,
    ) -> Result<Self, crate::error::Error> {
        let authority =
            GeographicCoverageSource::from_validated(authority, SphericalGridTarget::Rectangular)
                .map_err(|_| crate::error::ResourceError::Allocation)?;
        let working = NativeRectClassifier::new(working)?;
        let prepared =
            crate::geometry::GeographicPrepared::try_new(authority_plan, authority_cache, frame)?;
        Ok(Self {
            authority,
            working,
            prepared,
            authority_graph: OnceLock::new(),
        })
    }

    fn classify_bounds(&self, bounds: Bounds) -> RectClass {
        let working = self.working.classify_bounds(bounds);
        let candidate = self.authority.classify_rect(rectangle_windows(bounds));
        match (candidate, working) {
            (AffineRectClass::Outside, RectClass::Outside) => RectClass::Outside,
            (AffineRectClass::Interior, RectClass::Interior) => RectClass::Interior,
            _ => RectClass::Boundary,
        }
    }

    /// Refine a provisional rectangle against the source-owned geographic
    /// graph. A pre-certified interior remains on the fast path; every other
    /// candidate reaches the cached authority graph.
    fn refine_bounds(&self, bounds: Bounds) -> Result<RectClass, crate::error::Error> {
        let graph = self.authority_graph();
        Ok(match graph.covers_rectangle(bounds) {
            Some(true) => RectClass::Interior,
            Some(false) => {
                if graph_intersects_bounds(graph, bounds) {
                    RectClass::Boundary
                } else {
                    RectClass::Outside
                }
            },
            None => {
                let candidate = bounds_to_shape(bounds);
                let candidate_plan = PreparedPlan::transient(&candidate);
                let candidate_cache = crate::geometry::FrameDependentCaches::default();
                let operands = PreparedTopologyPair::Geographic {
                    left: self.prepared,
                    right: crate::geometry::GeographicPrepared::try_new(
                        &candidate_plan,
                        &candidate_cache,
                        self.prepared.frame(),
                    )?,
                };
                if topology_scalar_pair(&Predicate::Covers.spec(), operands) {
                    RectClass::Interior
                } else if topology_scalar_pair(&Predicate::Intersects.spec(), operands) {
                    RectClass::Boundary
                } else {
                    RectClass::Outside
                }
            },
        })
    }

    fn authority_graph(&self) -> &crate::geometry::TopologyGraph {
        self.authority_graph
            .get_or_init(|| self.prepared.graph())
            .as_ref()
    }

    fn covers_point(&self, point: Point) -> bool {
        self.authority_graph().covers_point(point.xy())
    }
}

const fn rectangle_windows(bounds: Bounds) -> DegreeWindowResult {
    let (Some(longitude), Some(latitude)) = (
        Bound::new(bounds.minx(), bounds.maxx()),
        Bound::new(bounds.miny(), bounds.maxy()),
    ) else {
        return DegreeWindowResult::Boundary;
    };
    DegreeWindowResult::Windows(CertifiedDegreeWindows {
        latitude,
        longitude: CertifiedLongitudeDegrees::One(longitude),
    })
}

struct AreaRectClassifier {
    bounds: Option<Bounds>,
    tester: Arc<PointBatchTester>,
    boundary_index: SegmentIndex,
}

impl NativeRectClassifier {
    pub(crate) fn new(source: &Shape) -> Result<Self, crate::error::Error> {
        Self::new_with_area_tester(source, None)
    }

    /// Build with a reusable polygonal point tester when the caller already
    /// prepared one for the same areal source (S2 does this for vertex and
    /// center probes). Non-polygonal/mixed sources keep an independently
    /// prepared tester because their dissolved-area semantics differ.
    pub(crate) fn new_with_area_tester(
        source: &Shape,
        area_tester: Option<Arc<PointBatchTester>>,
    ) -> Result<Self, crate::error::Error> {
        let mut polygons = Vec::new();
        collect_area(source, &mut polygons);
        let area = dissolved_area_shape(polygons)?.map(|shape| {
            let tester = area_tester.unwrap_or_else(|| Arc::new(PointBatchTester::new(&shape)));
            AreaRectClassifier {
                bounds: shape.bounds(),
                tester,
                boundary_index: polygon_segment_index(&shape),
            }
        });

        let mut line_seqs = Vec::new();
        collect_line_sequences(source, &mut line_seqs);
        let line_index = (!line_seqs.is_empty())
            .then(|| SegmentIndex::build_from_iter(line_seqs.into_iter().flat_map(line_segments)));

        let mut points = Vec::new();
        collect_points(source, &mut points);

        Ok(Self {
            source_bounds: source.bounds(),
            area,
            line_index,
            points: points.into_boxed_slice(),
        })
    }

    /// Classify a closed lon/lat rectangle against the source. Polygonal
    /// area may certify `Interior`; lineal and point components can only
    /// make an otherwise outside rectangle `Boundary`.
    pub(crate) fn classify_bounds(&self, bounds: Bounds) -> RectClass {
        let Some(source_bounds) = self.source_bounds else {
            return RectClass::Outside;
        };
        if !bounds.intersects(source_bounds) {
            return RectClass::Outside;
        }

        if let Some(area) = &self.area {
            match area.classify_bounds(bounds) {
                RectClass::Interior => return RectClass::Interior,
                RectClass::Boundary => return RectClass::Boundary,
                RectClass::Outside => {},
            }
        }

        if self.line_intersects(bounds) || self.point_intersects(bounds) {
            RectClass::Boundary
        } else {
            RectClass::Outside
        }
    }

    fn line_intersects(&self, bounds: Bounds) -> bool {
        let Some(index) = &self.line_index else {
            return false;
        };
        let probe = rect_probe(bounds);
        index
            .intersecting_candidates(probe)
            .any(|entry| segment_vs_rect(entry.segment, bounds) != RectContact::None)
    }

    fn point_intersects(&self, bounds: Bounds) -> bool {
        self.points.iter().any(|point| {
            point.x >= bounds.minx()
                && point.x <= bounds.maxx()
                && point.y >= bounds.miny()
                && point.y <= bounds.maxy()
        })
    }
}

impl AreaRectClassifier {
    fn classify_bounds(&self, bounds: Bounds) -> RectClass {
        let Some(area_bounds) = self.bounds else {
            return RectClass::Outside;
        };
        if !bounds.intersects(area_bounds) {
            return RectClass::Outside;
        }

        let mut touches_closed = false;
        let probe = rect_probe(bounds);
        for entry in self.boundary_index.intersecting_candidates(probe) {
            match segment_vs_rect(entry.segment, bounds) {
                RectContact::Open => return RectClass::Boundary,
                RectContact::Closed => touches_closed = true,
                RectContact::None => {},
            }
        }
        let center = Point::new_raw_xy(
            f64::midpoint(bounds.minx(), bounds.maxx()),
            f64::midpoint(bounds.miny(), bounds.maxy()),
        );
        if self.tester.covers_point(center) {
            RectClass::Interior
        } else if touches_closed {
            RectClass::Boundary
        } else {
            RectClass::Outside
        }
    }
}

fn area_shape(polygons: Vec<Polygon>) -> Option<Shape> {
    (!polygons.is_empty())
        .then(|| Shape::narrow_polygons(crate::geometry::CoordinateAxes::XY, polygons))
}

fn dissolved_area_shape(polygons: Vec<Polygon>) -> Result<Option<Shape>, crate::error::Error> {
    if polygons.len() <= 1 {
        return Ok(area_shape(polygons));
    }
    let parts: Vec<_> = polygons.into_iter().map(Shape::Polygon).collect();
    let dissolved = Shape::union_all(&parts, Strictness::Strict)?;
    let mut polygons = Vec::new();
    collect_area(&dissolved, &mut polygons);
    Ok(area_shape(polygons))
}

/// Extract and dissolve the polygonal members of an arbitrary source.
///
/// Unlike the classifier's internal construction path, public callers can
/// propagate a topology failure instead of turning it into a panic.
pub(crate) fn dissolved_polygonal_area(
    source: &Shape,
) -> Result<Option<Shape>, crate::error::Error> {
    let mut polygons = Vec::new();
    collect_area(source, &mut polygons);
    dissolved_area_shape(polygons)
}

fn collect_area(source: &Shape, out: &mut Vec<Polygon>) {
    match source {
        Shape::Polygon(polygon) => out.push(polygon.clone()),
        Shape::MultiPolygon(polygons) => out.extend(polygons.iter().cloned()),
        Shape::GeometryCollection(geometries) => {
            for geometry in geometries {
                collect_area(geometry, out);
            }
        },
        _ => {},
    }
}

fn collect_line_sequences<'a>(source: &'a Shape, out: &mut Vec<&'a CoordSeq>) {
    match source {
        Shape::LineString(line) => out.push(line),
        Shape::MultiLineString(lines) => {
            for line in lines {
                out.push(line);
            }
        },
        Shape::GeometryCollection(geometries) => {
            for geometry in geometries {
                collect_line_sequences(geometry, out);
            }
        },
        _ => {},
    }
}

fn polygon_segment_index(source: &Shape) -> SegmentIndex {
    match source {
        Shape::Polygon(polygon) => {
            SegmentIndex::build_from_iter(polygon.rings().flat_map(line_segments))
        },
        Shape::MultiPolygon(polygons) => SegmentIndex::build_from_iter(
            polygons
                .iter()
                .flat_map(Polygon::rings)
                .flat_map(line_segments),
        ),
        _ => unreachable!("dissolved area is polygonal"),
    }
}

fn collect_points(source: &Shape, out: &mut Vec<Point>) {
    match source {
        Shape::Point(point) => out.push(*point),
        Shape::MultiPoint(points) => out.extend(points.iter()),
        Shape::GeometryCollection(geometries) => {
            for geometry in geometries {
                collect_points(geometry, out);
            }
        },
        _ => {},
    }
}

const fn rect_probe(bounds: Bounds) -> Segment {
    Segment {
        start: XY::new(bounds.minx(), bounds.miny()),
        end: XY::new(bounds.maxx(), bounds.maxy()),
    }
}

/// Cover `source` with `roots`' descendants at `target_depth`.
///
/// Candidate branches descend until their terminal refinement; provisional
/// boundaries receive authority-graph refinement before output.
/// `roots` must be DISJOINT cells (every caller passes
/// the system's base cells), so each target-depth cell is classified exactly
/// once and the outputs need ordering but never dedup.
///
/// Returns a sorted cell vector. `Overlap` retains interior and refined
/// boundary cells, `Center` retains interior cells and boundary cells whose
/// binary64 midpoint is covered, and `Within` retains only interior cells.
///
/// The emitted cell count is bounded by `max_cells` (when `Some`), checked at
/// every emission: a world-scale geometry at a fine `target_depth` fails with
/// [`CoverBudgetExceeded`] (naming `max_cells`) before the next allocation
/// instead of flooding memory. `None` = unlimited.
pub(crate) fn cover<C: RectCell>(
    authority: &Shape,
    working: &Shape,
    authority_plan: &PreparedPlan,
    authority_cache: &crate::geometry::FrameDependentCaches,
    frame: crate::geographic::GeographicFrame,
    rule: RectCoverRule,
    roots: Vec<C>,
    target_depth: u8,
    max_cells: Option<usize>,
) -> Result<Vec<C>, CoverError> {
    let classifier = GeographicCandidateClassifier::new(
        authority,
        working,
        authority_plan,
        authority_cache,
        frame,
    )?;
    let mut cells = Vec::new();
    // Carry bounds through every traversal step so the terminal can be
    // refined without recomputing a cell's rectangle.
    let mut boundary: Vec<(C, Bounds)> = roots
        .into_iter()
        .map(|cell| (cell, cell.bounds()))
        .collect();
    let mut interior: Vec<C> = Vec::new();
    let mut emit = |cell| {
        ensure_cover_budget(cells.len().saturating_add(1), max_cells)?;
        cells.push(cell);
        Ok::<_, crate::grid::CoverBudgetExceeded>(())
    };
    loop {
        while let Some(cell) = interior.pop() {
            if cell.depth() >= target_depth {
                emit(cell)?;
            } else {
                interior.extend(cell.children());
            }
        }
        let Some((cell, bounds)) = boundary.pop() else {
            break;
        };
        match classifier.classify_bounds(bounds) {
            RectClass::Outside => {},
            RectClass::Interior => {
                if cell.depth() >= target_depth {
                    emit(cell)?;
                } else {
                    interior.extend(cell.children());
                }
            },
            RectClass::Boundary => {
                match classifier.refine_bounds(bounds)? {
                    RectClass::Outside => {},
                    RectClass::Interior => {
                        if cell.depth() >= target_depth {
                            emit(cell)?;
                        } else {
                            interior.extend(cell.children());
                        }
                    },
                    RectClass::Boundary => {
                        if cell.depth() >= target_depth {
                            let visible = match rule {
                                RectCoverRule::Overlap => true,
                                RectCoverRule::Center => {
                                    let center = Point::new_raw_xy(
                                        f64::midpoint(bounds.minx(), bounds.maxx()),
                                        f64::midpoint(bounds.miny(), bounds.maxy()),
                                    );
                                    classifier.covers_point(center)
                                },
                                RectCoverRule::Within => false,
                            };
                            if visible {
                                emit(cell)?;
                            }
                        } else {
                            // Expand in place onto `boundary` — avoid a side buffer
                            // that copies twice. Children derived from parent edges
                            // (Tile: one shared mid-lat) rather than full recompute.
                            cell.push_children_bounds(bounds, &mut boundary);
                        }
                    },
                }
            },
        }
    }
    cells.sort_unstable();
    Ok(cells)
}

/// Where a candidate cell sits relative to the source geometry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum CellClass {
    Outside,
    Boundary,
    Interior,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;
    use crate::geometry::GeometryErrorKind;
    use crate::grid::geohash;

    fn contact(start: (f64, f64), end: (f64, f64)) -> RectContact {
        let unit = Bounds::new_raw(0.0, 0.0, 1.0, 1.0);
        let segment = crate::geometry::Segment {
            start: crate::geometry::XY::new(start.0, start.1),
            end: crate::geometry::XY::new(end.0, end.1),
        };
        segment_vs_rect(segment, unit)
    }

    #[test]
    fn segment_vs_rect_contact_classes_are_exact() {
        use RectContact::{Closed, None, Open};
        // Envelope separation.
        assert!(matches!(contact((2.0, 0.5), (3.0, 0.5)), None));
        // SEPARATING CARRIER: bbox overlaps the rect, but every corner is
        // strictly one side of the line — no contact (the round-26 bug
        // returned Closed here, demoting covered cells to boundary).
        assert!(matches!(contact((-1.0, 0.5), (0.4, 1.9)), None));
        // Proper crossings (interior entry), including axis-parallel
        // carriers through the interior.
        assert!(matches!(contact((-1.0, 0.5), (2.0, 0.5)), Open));
        assert!(matches!(contact((0.5, -1.0), (0.5, 2.0)), Open));
        assert!(matches!(contact((-0.5, -0.5), (1.5, 1.5)), Open));
        // One endpoint strictly inside.
        assert!(matches!(contact((0.5, 0.5), (2.0, 2.0)), Open));
        // Corner-only touch: the carrier supports the rect at (0, 1).
        assert!(matches!(contact((-0.5, 0.5), (0.5, 1.5)), Closed));
        // Collinear run along an edge, and an endpoint resting on an edge.
        assert!(matches!(contact((-0.5, 1.0), (0.5, 1.0)), Closed));
        assert!(matches!(contact((-1.0, 0.5), (0.0, 0.5)), Closed));
        // Crossing carrier whose segment extent only reaches the boundary.
        assert!(matches!(contact((-1.0, -1.0), (0.0, 0.5)), Closed));
        // Degenerate segments (repeated ring vertices): point-in-open-rect
        // is Open, on the closed boundary Closed, outside None.
        assert!(matches!(contact((0.5, 0.5), (0.5, 0.5)), Open));
        assert!(matches!(contact((0.0, 0.5), (0.0, 0.5)), Closed));
        assert!(matches!(contact((1.0, 1.0), (1.0, 1.0)), Closed));
        assert!(matches!(contact((1.5, 0.5), (1.5, 0.5)), None));
    }

    #[test]
    fn classifier_propagates_invalid_dissolved_area() {
        use crate::geometry::Ring;

        let z_square = Polygon::new(
            Ring::from_closed(vec![
                Point::new_raw_axes(
                    0.0,
                    0.0,
                    crate::geometry::ZOrdinate(Some(1.0)),
                    crate::geometry::MOrdinate(None),
                ),
                Point::new_raw_axes(
                    2.0,
                    0.0,
                    crate::geometry::ZOrdinate(Some(1.0)),
                    crate::geometry::MOrdinate(None),
                ),
                Point::new_raw_axes(
                    2.0,
                    2.0,
                    crate::geometry::ZOrdinate(Some(1.0)),
                    crate::geometry::MOrdinate(None),
                ),
                Point::new_raw_axes(
                    0.0,
                    2.0,
                    crate::geometry::ZOrdinate(Some(1.0)),
                    crate::geometry::MOrdinate(None),
                ),
                Point::new_raw_axes(
                    0.0,
                    0.0,
                    crate::geometry::ZOrdinate(Some(1.0)),
                    crate::geometry::MOrdinate(None),
                ),
            ]),
            Vec::new(),
        );
        let square = Polygon::new(
            Ring::from_closed(vec![
                Point::new_raw_xy(1.0, 1.0),
                Point::new_raw_xy(3.0, 1.0),
                Point::new_raw_xy(3.0, 3.0),
                Point::new_raw_xy(1.0, 3.0),
                Point::new_raw_xy(1.0, 1.0),
            ]),
            Vec::new(),
        );
        let source =
            Shape::geometry_collection(vec![Shape::Polygon(z_square), Shape::Polygon(square)])
                .expect("test collection is valid");
        let authority_plan = PreparedPlan::new(source.clone());
        let authority_cache = crate::geometry::FrameDependentCaches::default();

        let error = match cover(
            &source,
            &source,
            &authority_plan,
            &authority_cache,
            crate::geographic::GeographicFrame::crs84(),
            RectCoverRule::Overlap,
            geohash::roots(),
            1,
            None,
        ) {
            Ok(_) => panic!("cover preparation must reject dropped ordinates"),
            Err(CoverError::Preparation(error)) => error,
            Err(CoverError::Budget(error)) => panic!("unexpected budget error: {error}"),
        };
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::OrdinateDropped(_))
        ));
    }

    /// A near-world polygon covered at a fine depth expands past the shared
    /// cell budget; the coverer fails deterministically (naming its depth
    /// parameter) during descendant emission rather than flooding memory.
    #[test]
    fn cover_rejects_world_scale_fine_depth_before_flooding() {
        use crate::geometry::Ring;
        let shell = vec![
            Point::new_raw_xy(-179.0, -85.0),
            Point::new_raw_xy(179.0, -85.0),
            Point::new_raw_xy(179.0, 85.0),
            Point::new_raw_xy(-179.0, 85.0),
            Point::new_raw_xy(-179.0, -85.0),
        ];
        let world = Shape::Polygon(Polygon::new(Ring::from_closed(shell), Vec::new()));
        let authority_plan = PreparedPlan::new(world.clone());
        let authority_cache = crate::geometry::FrameDependentCaches::default();
        match cover(
            &world,
            &world,
            &authority_plan,
            &authority_cache,
            crate::geographic::GeographicFrame::crs84(),
            RectCoverRule::Overlap,
            geohash::roots(),
            10,
            Some(usize::try_from(crate::grid::GRID_MAX_CELLS).unwrap()),
        ) {
            Ok(_) => panic!("world at precision 10 must exceed the cell budget"),
            Err(CoverError::Budget(err)) => {
                assert_eq!(
                    err.limit,
                    usize::try_from(crate::grid::GRID_MAX_CELLS).unwrap()
                );
                assert!(err.to_string().contains("max_cells"));
            },
            Err(CoverError::Preparation(err)) => panic!("unexpected preparation error: {err}"),
        }
    }

    #[test]
    fn center_rule_emits_only_center_covered_cells() {
        use crate::geometry::Ring;
        // Unit square: center-rule at coarse precision should match
        // overlap-then-filter on the same classifier.
        let shell = vec![
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(1.0, 0.0),
            Point::new_raw_xy(1.0, 1.0),
            Point::new_raw_xy(0.0, 1.0),
            Point::new_raw_xy(0.0, 0.0),
        ];
        let poly = Shape::Polygon(Polygon::new(Ring::from_closed(shell), Vec::new()));
        let authority_plan = PreparedPlan::new(poly.clone());
        let authority_cache = crate::geometry::FrameDependentCaches::default();
        let depth = 3_u8;
        let center = cover(
            &poly,
            &poly,
            &authority_plan,
            &authority_cache,
            crate::geographic::GeographicFrame::crs84(),
            RectCoverRule::Center,
            geohash::roots(),
            depth,
            None,
        )
        .expect("cover");
        let overlap = cover(
            &poly,
            &poly,
            &authority_plan,
            &authority_cache,
            crate::geographic::GeographicFrame::crs84(),
            RectCoverRule::Overlap,
            geohash::roots(),
            depth,
            None,
        )
        .expect("cover");
        let graph = authority_cache.geographic_topology_graph(
            crate::geometry::GeographicPrepared::try_new(
                &authority_plan,
                &authority_cache,
                crate::geographic::GeographicFrame::crs84(),
            )
            .expect("fixture admits")
            .view(),
        );
        let filtered: Vec<_> = overlap
            .into_iter()
            .filter_map(|cell| {
                let bounds = cell.bounds();
                let c = Point::new_raw_xy(
                    f64::midpoint(bounds.minx(), bounds.maxx()),
                    f64::midpoint(bounds.miny(), bounds.maxy()),
                );
                graph.covers_point(c.xy()).then_some(cell)
            })
            .collect();
        assert_eq!(center, filtered);
    }

    #[test]
    fn rectangle_refinement_reuses_one_authority_graph() {
        use crate::geometry::Ring;

        let source = Shape::Polygon(Polygon::new(
            Ring::from_closed(vec![
                Point::new_raw_xy(-10.0, 80.0),
                Point::new_raw_xy(10.0, 80.0),
                Point::new_raw_xy(10.0, 90.0),
                Point::new_raw_xy(-10.0, 90.0),
                Point::new_raw_xy(-10.0, 80.0),
            ]),
            Vec::new(),
        ));
        let plan = PreparedPlan::new(source.clone());
        let cache = crate::geometry::FrameDependentCaches::default();
        let classifier = GeographicCandidateClassifier::new(
            &source,
            &source,
            &plan,
            &cache,
            crate::geographic::GeographicFrame::crs84(),
        )
        .unwrap();
        let bounds = Bounds::new_raw(0.0, 81.0, 1.0, 82.0);
        assert_eq!(
            classifier.refine_bounds(bounds).expect("fixture admits"),
            RectClass::Interior
        );
        assert_eq!(
            classifier.refine_bounds(bounds).expect("fixture admits"),
            RectClass::Interior
        );
        assert_eq!(cache.geographic_topology_accesses(), 1);
        assert_eq!(cache.geographic_topology_builds(), 1);
    }

    #[test]
    fn closed_edge_pentagon_cells_refine_to_interior() {
        use crate::geometry::Ring;

        let source = Shape::Polygon(Polygon::new(
            Ring::from_closed(vec![
                Point::new_raw_xy(-50.0, 0.0),
                Point::new_raw_xy(50.0, 0.0),
                Point::new_raw_xy(50.0, 50.0),
                Point::new_raw_xy(0.0, 60.0),
                Point::new_raw_xy(-50.0, 50.0),
                Point::new_raw_xy(-50.0, 0.0),
            ]),
            Vec::new(),
        ));
        let plan = PreparedPlan::new(source.clone());
        let cache = crate::geometry::FrameDependentCaches::default();
        let classifier = GeographicCandidateClassifier::new(
            &source,
            &source,
            &plan,
            &cache,
            crate::geographic::GeographicFrame::crs84(),
        )
        .unwrap();

        for token in ["e", "s"] {
            let cell = geohash::Geohash::parse(token).unwrap();
            assert_eq!(
                classifier
                    .refine_bounds(cell.bounds())
                    .expect("fixture admits"),
                RectClass::Interior,
                "cell {token}"
            );
        }
        let cells = cover(
            &source,
            &source,
            &plan,
            &cache,
            crate::geographic::GeographicFrame::crs84(),
            RectCoverRule::Within,
            geohash::roots(),
            1,
            None,
        )
        .unwrap();
        assert_eq!(cells.iter().map(|cell| cell.token()).collect::<Vec<_>>(), [
            "e", "s"
        ]);
    }

    #[test]
    fn exact_hole_cell_stays_outside_after_graph_refinement() {
        use crate::geometry::Ring;

        let source = Shape::Polygon(Polygon::new(
            Ring::from_closed(vec![
                Point::new_raw_xy(-45.0, -45.0),
                Point::new_raw_xy(45.0, -45.0),
                Point::new_raw_xy(45.0, 45.0),
                Point::new_raw_xy(-45.0, 45.0),
                Point::new_raw_xy(-45.0, -45.0),
            ]),
            vec![Ring::from_closed(vec![
                Point::new_raw_xy(-45.0, -45.0),
                Point::new_raw_xy(0.0, -45.0),
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(-45.0, 0.0),
                Point::new_raw_xy(-45.0, -45.0),
            ])],
        ));
        let plan = PreparedPlan::new(source.clone());
        let cache = crate::geometry::FrameDependentCaches::default();
        let classifier = GeographicCandidateClassifier::new(
            &source,
            &source,
            &plan,
            &cache,
            crate::geographic::GeographicFrame::crs84(),
        )
        .unwrap();
        let bounds = geohash::Geohash::parse("7").unwrap().bounds();

        assert_eq!(classifier.classify_bounds(bounds), RectClass::Boundary);
        assert_eq!(
            classifier.authority_graph().covers_rectangle(bounds),
            Some(false)
        );
        assert_eq!(
            classifier.refine_bounds(bounds).expect("fixture admits"),
            RectClass::Boundary
        );
    }

    #[test]
    fn unsupported_rectangle_refinement_distinguishes_cover_and_intersection() {
        use crate::geometry::Ring;

        let source = Shape::Polygon(Polygon::new(
            Ring::from_closed(vec![
                Point::new_raw_xy(170.0, 80.0),
                Point::new_raw_xy(-170.0, 80.0),
                Point::new_raw_xy(-170.0, 90.0),
                Point::new_raw_xy(170.0, 90.0),
                Point::new_raw_xy(170.0, 80.0),
            ]),
            Vec::new(),
        ));
        let plan = PreparedPlan::new(source.clone());
        let cache = crate::geometry::FrameDependentCaches::default();
        let classifier = GeographicCandidateClassifier::new(
            &source,
            &source,
            &plan,
            &cache,
            crate::geographic::GeographicFrame::crs84(),
        )
        .unwrap();
        let disjoint = Bounds::new_raw(-180.0, 70.0, -170.0, 79.0);
        let intersects = Bounds::new_raw(-180.0, 82.7, -165.0, 85.0);
        let covers = Bounds::new_raw(170.0, 81.0, 179.0, 90.0);

        for bounds in [disjoint, intersects, covers] {
            assert_eq!(classifier.authority_graph().covers_rectangle(bounds), None);
        }
        assert_eq!(
            classifier.refine_bounds(disjoint).expect("fixture admits"),
            RectClass::Outside
        );
        assert_eq!(
            classifier
                .refine_bounds(intersects)
                .expect("fixture admits"),
            RectClass::Boundary
        );
        assert_eq!(
            classifier.refine_bounds(covers).expect("fixture admits"),
            RectClass::Interior
        );
    }

    #[test]
    fn periodic_authority_and_internal_chart_emit_the_same_candidates() {
        use crate::geometry::Ring;

        let raw = Shape::Polygon(Polygon::new(
            Ring::from_closed(vec![
                Point::new_raw_xy(179.0, -1.0),
                Point::new_raw_xy(-179.0, -1.0),
                Point::new_raw_xy(-179.0, 1.0),
                Point::new_raw_xy(179.0, 1.0),
                Point::new_raw_xy(179.0, -1.0),
            ]),
            Vec::new(),
        ));
        let chart = raw.split_antimeridian().unwrap();
        let raw_plan = PreparedPlan::new(raw.clone());
        let raw_cache = crate::geometry::FrameDependentCaches::default();
        let chart_plan = PreparedPlan::new(chart.clone());
        let chart_cache = crate::geometry::FrameDependentCaches::default();
        let from_raw = cover(
            &raw,
            &chart,
            &raw_plan,
            &raw_cache,
            crate::geographic::GeographicFrame::crs84(),
            RectCoverRule::Overlap,
            geohash::roots(),
            3,
            None,
        )
        .unwrap();
        let from_chart = cover(
            &chart,
            &chart,
            &chart_plan,
            &chart_cache,
            crate::geographic::GeographicFrame::crs84(),
            RectCoverRule::Overlap,
            geohash::roots(),
            3,
            None,
        )
        .unwrap();
        assert_eq!(from_raw, from_chart);
    }
}
