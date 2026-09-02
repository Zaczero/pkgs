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

use std::sync::Arc;

use crate::geometry::{
    Bounds, CoordSeq, GeographicPrepared, Point, PointBatchTester, Polygon, PreparedPlan, Segment,
    SegmentIndex, Shape, Strictness, XY, bounds_to_shape, graph_intersects_rectangle_data,
    line_segments,
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
    exact: bool,
    authority_plan: &'a PreparedPlan,
    authority_cache: &'a crate::geometry::FrameDependentCaches,
    frame: crate::geographic::GeographicFrame,
}

impl<'a> GeographicCandidateClassifier<'a> {
    fn new(
        authority: &Shape,
        working: &Shape,
        charted: bool,
        authority_plan: &'a PreparedPlan,
        authority_cache: &'a crate::geometry::FrameDependentCaches,
        frame: crate::geographic::GeographicFrame,
    ) -> Result<Self, crate::error::Error> {
        Ok(Self {
            authority: GeographicCoverageSource::from_validated(
                authority,
                SphericalGridTarget::Rectangular,
            )
            .map_err(|_| crate::error::ResourceError::Allocation)?,
            working: NativeRectClassifier::new(working)?,
            exact: crate::geometry::shape_reaches_geographic_pole_in_frame(authority, frame)
                || crate::geometry::shape_spans_full_longitude(authority)
                || (charted
                    && authority
                        .bounds()
                        .is_some_and(|bounds| bounds.maxx() - bounds.minx() <= 180.0)),
            authority_plan,
            authority_cache,
            frame,
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

    /// Refine a quotient-sensitive retained rectangle against the source-owned
    /// geographic graph. The affine source and working chart admit ordinary
    /// candidates without rebuilding the source graph.
    fn exact_classify_bounds(&self, bounds: Bounds, proposed: RectClass) -> RectClass {
        if !self.exact || proposed == RectClass::Interior {
            return proposed;
        }
        let candidate = bounds_to_shape(bounds);
        let candidate_plan = PreparedPlan::transient(&candidate);
        let candidate_cache = crate::geometry::FrameDependentCaches::default();
        if !self.exact_intersects_bounds(&candidate, proposed) {
            return RectClass::Outside;
        }
        let operands = PreparedTopologyPair::for_domain(
            self.authority_plan,
            self.authority_cache,
            &candidate_plan,
            &candidate_cache,
            crate::geometry::TopologyDomain::Geographic(self.frame),
        );
        if topology_scalar_pair(&Predicate::Covers.spec(), operands) {
            RectClass::Interior
        } else {
            RectClass::Boundary
        }
    }

    fn exact_intersects_bounds(&self, candidate: &Shape, proposed: RectClass) -> bool {
        if !self.exact || proposed == RectClass::Interior {
            return true;
        }
        graph_intersects_rectangle_data(
            self.authority_plan,
            None,
            Some(GeographicPrepared::new(
                self.authority_plan,
                self.authority_cache,
                self.frame,
            )),
            candidate,
        )
        .unwrap_or(true)
    }

    fn exact_covers_point(&self, point: Point) -> bool {
        if !self.exact {
            return self.working.covers_point(point);
        }
        let candidate = PreparedPlan::transient(&Shape::Point(point));
        let candidate_cache = crate::geometry::FrameDependentCaches::default();
        topology_scalar_pair(
            &Predicate::Covers.spec(),
            PreparedTopologyPair::for_domain(
                self.authority_plan,
                self.authority_cache,
                &candidate,
                &candidate_cache,
                crate::geometry::TopologyDomain::Geographic(self.frame),
            ),
        )
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

    /// Boundary-inclusive point membership against the classified source
    /// components (areal tester + linework + points). Matches
    /// [`Shape::covers_point`] for the parts collected at construction.
    pub(crate) fn covers_point(&self, point: Point) -> bool {
        if let Some(area) = &self.area
            && area.tester.covers_point(point)
        {
            return true;
        }
        if self.line_covers_point(point) {
            return true;
        }
        self.points.iter().any(|p| {
            // Exact coordinate identity for discrete point components.
            // Plain `==` so −0.0 and +0.0 match (to_bits would miss signed-zero
            // centers under half-open / center-cover classification).
            #[expect(clippy::float_cmp, reason = "signed-zero-aware point identity")]
            {
                p.x == point.x && p.y == point.y
            }
        })
    }

    fn line_covers_point(&self, point: Point) -> bool {
        let Some(index) = &self.line_index else {
            return false;
        };
        // Degenerate probe envelope around the point for candidate selection.
        let probe = Segment {
            start: XY::new(point.x, point.y),
            end: XY::new(point.x, point.y),
        };
        index.intersecting_candidates(probe).any(|entry| {
            crate::geometry::point_on_segment(point, entry.segment.start, entry.segment.end)
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
    match polygons.len() {
        0 => None,
        1 => polygons.into_iter().next().map(Shape::Polygon),
        _ => Some(Shape::MultiPolygon(polygons.into())),
    }
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

/// How a segment meets a rectangle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RectContact {
    /// Enters the rectangle's interior.
    Open,
    /// Touches the closed rectangle (edge/corner contact only).
    Closed,
    /// No contact.
    None,
}

/// Exact segment-vs-rectangle contact via the separating-axis test on
/// ROBUST orientations: the segment misses the open rect when its envelope
/// does not strictly overlap it, or when all four rect corners sit weakly
/// on one side of the segment's carrier line; closed contact then
/// distinguishes touch from miss with the weak forms.
pub(crate) fn segment_vs_rect(segment: Segment, rect: Bounds) -> RectContact {
    use crate::geometry::{Orientation, orientation};
    let (sx, ex) = (segment.start.x, segment.end.x);
    let (sy, ey) = (segment.start.y, segment.end.y);
    let (lo_x, hi_x) = (sx.min(ex), sx.max(ex));
    let (lo_y, hi_y) = (sy.min(ey), sy.max(ey));
    // Closed-envelope separation: no contact at all.
    if hi_x < rect.minx() || lo_x > rect.maxx() || hi_y < rect.miny() || lo_y > rect.maxy() {
        return RectContact::None;
    }
    // A degenerate segment (repeated ring vertex) is a point: every corner
    // orientation is Collinear, so decide it directly — inside the open
    // rect is Open, on the closed boundary (all that survives the envelope
    // rejection) is Closed. Numeric equality on purpose: it matches when
    // `orientation` degenerates (orient2d reads -0.0 as 0.0).
    #[expect(
        clippy::float_cmp,
        reason = "exact coordinate identity is required to classify a repeated segment endpoint"
    )]
    let degenerate = sx == ex && sy == ey;
    if degenerate {
        return if sx > rect.minx() && sx < rect.maxx() && sy > rect.miny() && sy < rect.maxy() {
            RectContact::Open
        } else {
            RectContact::Closed
        };
    }
    let corners = [
        XY::new(rect.minx(), rect.miny()),
        XY::new(rect.maxx(), rect.miny()),
        XY::new(rect.maxx(), rect.maxy()),
        XY::new(rect.minx(), rect.maxy()),
    ];
    let mut strict_positive = false;
    let mut strict_negative = false;
    let mut collinear = false;
    for corner in corners {
        match orientation(segment.start, segment.end, corner) {
            Orientation::CounterClockwise => strict_positive = true,
            Orientation::Clockwise => strict_negative = true,
            Orientation::Collinear => collinear = true,
        }
    }
    // All corners weakly on one side: the carrier never enters the open
    // rect. A collinear corner means the carrier SUPPORTS the rect there
    // (corner or whole edge), and the surviving closed-envelope overlap
    // pins the segment to that contact set — a closed touch. With every
    // corner strictly one side the carrier is a separating line.
    if !(strict_positive && strict_negative) {
        return if collinear {
            RectContact::Closed
        } else {
            RectContact::None
        };
    }
    // The carrier separates the corners; the open rect is entered iff the
    // SEGMENT EXTENT strictly overlaps the rect on both axes (a corner-only
    // or edge-only reach has a degenerate strict overlap).
    if hi_x > rect.minx() && lo_x < rect.maxx() && hi_y > rect.miny() && lo_y < rect.maxy() {
        RectContact::Open
    } else {
        RectContact::Closed
    }
}

/// Cover `source` with `roots`' descendants at `target_depth`.
///
/// Candidate branches descend until their terminal refinement; quotient-
/// sensitive retained candidates receive exact graph refinement before output.
/// `roots` must be DISJOINT cells (every caller passes
/// the system's base cells), so each target-depth cell is classified exactly
/// once and the outputs need ordering but never dedup.
///
/// Returns a sorted `(cell, is_interior)` stream — ready for
/// [`CoveragePartition::from_sorted_tagged`] without a second sort.
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
    charted: bool,
    roots: Vec<C>,
    target_depth: u8,
    max_cells: Option<usize>,
) -> Result<Vec<(C, bool)>, CoverError> {
    let classifier = GeographicCandidateClassifier::new(
        authority,
        working,
        charted,
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
    loop {
        while let Some(cell) = interior.pop() {
            if cell.depth() >= target_depth {
                ensure_cover_budget(cells.len().saturating_add(1), max_cells)?;
                cells.push((cell, true));
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
                match classifier.exact_classify_bounds(bounds, RectClass::Interior) {
                    RectClass::Outside => {},
                    RectClass::Interior => {
                        if cell.depth() >= target_depth {
                            ensure_cover_budget(cells.len().saturating_add(1), max_cells)?;
                            cells.push((cell, true));
                        } else {
                            interior.extend(cell.children());
                        }
                    },
                    RectClass::Boundary => {
                        if cell.depth() >= target_depth {
                            ensure_cover_budget(cells.len().saturating_add(1), max_cells)?;
                            cells.push((cell, false));
                        } else {
                            cell.push_children_bounds(bounds, &mut boundary);
                        }
                    },
                }
            },
            RectClass::Boundary => {
                if cell.depth() >= target_depth {
                    if !classifier
                        .exact_intersects_bounds(&bounds_to_shape(bounds), RectClass::Boundary)
                    {
                        continue;
                    }
                    // A boundary cell at the target depth intersects but is
                    // not fully covered.
                    ensure_cover_budget(cells.len().saturating_add(1), max_cells)?;
                    cells.push((cell, false));
                } else {
                    // Expand in place onto `boundary` — avoid a side buffer
                    // that copies twice. Children derived from parent edges
                    // (Tile: one shared mid-lat) rather than full recompute.
                    let start = boundary.len();
                    cell.push_children_bounds(bounds, &mut boundary);
                    // push_children_bounds appends; nothing else to do.
                    let _ = start;
                }
            },
        }
    }
    cells.sort_unstable_by_key(|(cell, _)| *cell);
    Ok(cells)
}

/// Cover `source` with target-depth cells whose **centers** are covered.
///
/// Descent:
/// - `Outside` → prune
/// - certified `Interior` → bulk-emit every target-depth descendant (center
///   covered by the interior certificate, no probe)
/// - `Boundary` → descend; at **target depth only**, emit iff the cell center
///   is covered
///
/// `max_cells` counts only visible emitted center cells (not a superset overlap
/// product). Returns a sorted unique cell vector.
pub(crate) fn cover_center<C: RectCell>(
    authority: &Shape,
    working: &Shape,
    authority_plan: &PreparedPlan,
    authority_cache: &crate::geometry::FrameDependentCaches,
    frame: crate::geographic::GeographicFrame,
    charted: bool,
    roots: Vec<C>,
    target_depth: u8,
    max_cells: Option<usize>,
) -> Result<Vec<C>, CoverError> {
    let classifier = GeographicCandidateClassifier::new(
        authority,
        working,
        charted,
        authority_plan,
        authority_cache,
        frame,
    )?;
    let mut cells = Vec::new();
    let mut boundary: Vec<(C, Bounds)> = roots
        .into_iter()
        .map(|cell| (cell, cell.bounds()))
        .collect();
    let mut interior: Vec<C> = Vec::new();
    loop {
        while let Some(cell) = interior.pop() {
            if cell.depth() >= target_depth {
                ensure_cover_budget(cells.len().saturating_add(1), max_cells)?;
                cells.push(cell);
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
                match classifier.exact_classify_bounds(bounds, RectClass::Interior) {
                    RectClass::Outside => {},
                    RectClass::Interior => {
                        if cell.depth() >= target_depth {
                            ensure_cover_budget(cells.len().saturating_add(1), max_cells)?;
                            cells.push(cell);
                        } else {
                            interior.extend(cell.children());
                        }
                    },
                    RectClass::Boundary => {
                        if cell.depth() >= target_depth {
                            let center = Point::new_raw_xy(
                                f64::midpoint(bounds.minx(), bounds.maxx()),
                                f64::midpoint(bounds.miny(), bounds.maxy()),
                            );
                            if classifier.exact_covers_point(center) {
                                ensure_cover_budget(cells.len().saturating_add(1), max_cells)?;
                                cells.push(cell);
                            }
                        } else {
                            cell.push_children_bounds(bounds, &mut boundary);
                        }
                    },
                }
            },
            RectClass::Boundary => {
                if cell.depth() >= target_depth {
                    let center = Point::new_raw_xy(
                        f64::midpoint(bounds.minx(), bounds.maxx()),
                        f64::midpoint(bounds.miny(), bounds.maxy()),
                    );
                    if classifier.exact_covers_point(center) {
                        ensure_cover_budget(cells.len().saturating_add(1), max_cells)?;
                        cells.push(cell);
                    }
                } else {
                    cell.push_children_bounds(bounds, &mut boundary);
                }
            },
        }
    }
    cells.sort_unstable();
    Ok(cells)
}

/// Cover `source` with target-depth cells fully contained by it.
///
/// Boundary descendants remain traversal state but are never retained or
/// charged to `max_cells`; only visible interior cells consume the output
/// budget.
pub(crate) fn cover_within<C: RectCell>(
    authority: &Shape,
    working: &Shape,
    authority_plan: &PreparedPlan,
    authority_cache: &crate::geometry::FrameDependentCaches,
    frame: crate::geographic::GeographicFrame,
    charted: bool,
    roots: Vec<C>,
    target_depth: u8,
    max_cells: Option<usize>,
) -> Result<Vec<C>, CoverError> {
    let classifier = GeographicCandidateClassifier::new(
        authority,
        working,
        charted,
        authority_plan,
        authority_cache,
        frame,
    )?;
    let mut cells = Vec::new();
    let mut boundary: Vec<(C, Bounds)> = roots
        .into_iter()
        .map(|cell| (cell, cell.bounds()))
        .collect();
    let mut interior: Vec<C> = Vec::new();
    loop {
        while let Some(cell) = interior.pop() {
            if cell.depth() >= target_depth {
                ensure_cover_budget(cells.len().saturating_add(1), max_cells)?;
                cells.push(cell);
            } else {
                interior.extend(cell.children());
            }
        }
        let Some((cell, bounds)) = boundary.pop() else {
            break;
        };
        match classifier.classify_bounds(bounds) {
            RectClass::Interior => {
                match classifier.exact_classify_bounds(bounds, RectClass::Interior) {
                    RectClass::Interior => {
                        if cell.depth() >= target_depth {
                            ensure_cover_budget(cells.len().saturating_add(1), max_cells)?;
                            cells.push(cell);
                        } else {
                            interior.extend(cell.children());
                        }
                    },
                    RectClass::Boundary if cell.depth() < target_depth => {
                        cell.push_children_bounds(bounds, &mut boundary);
                    },
                    RectClass::Outside | RectClass::Boundary => {},
                }
            },
            RectClass::Boundary if cell.depth() < target_depth => {
                cell.push_children_bounds(bounds, &mut boundary);
            },
            RectClass::Outside | RectClass::Boundary => {},
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

/// Pre-patch cover: recompute `cell.bounds()` every frame, single mixed stack.
/// Test-only dual-path baseline for the bounds-carry optimization.
#[cfg(test)]
pub(crate) fn cover_recompute_baseline<C: RectCell>(
    source: &Shape,
    roots: Vec<C>,
    target_depth: u8,
    max_cells: Option<usize>,
) -> Result<Vec<(C, bool)>, CoverError> {
    let classifier = NativeRectClassifier::new(source)?;
    let mut cells = Vec::new();
    let mut stack: Vec<(C, bool)> = roots.into_iter().map(|cell| (cell, false)).collect();
    while let Some((cell, inherited_interior)) = stack.pop() {
        let class = if inherited_interior {
            RectClass::Interior
        } else {
            classifier.classify_bounds(cell.bounds())
        };
        match class {
            RectClass::Outside => {},
            RectClass::Interior => {
                if cell.depth() >= target_depth {
                    cells.push((cell, true));
                    ensure_cover_budget(cells.len(), max_cells)?;
                } else {
                    stack.extend(cell.children().map(|child| (child, true)));
                }
            },
            RectClass::Boundary => {
                if cell.depth() >= target_depth {
                    cells.push((cell, false));
                    ensure_cover_budget(cells.len(), max_cells)?;
                } else {
                    stack.extend(cell.children().map(|child| (child, false)));
                }
            },
        }
    }
    cells.sort_unstable_by_key(|(cell, _)| *cell);
    Ok(cells)
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
            false,
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

    #[test]
    fn cover_bounds_carry_matches_recompute() {
        use crate::geometry::Ring;
        use crate::grid::tile;
        // Boundary-heavy lat band (z15) — the investigation's public case shape.
        let shell = vec![
            Point::new_raw_xy(-120.0, 30.0),
            Point::new_raw_xy(-60.0, 30.0),
            Point::new_raw_xy(-60.0, 30.5),
            Point::new_raw_xy(-120.0, 30.5),
            Point::new_raw_xy(-120.0, 30.0),
        ];
        let band = Shape::Polygon(Polygon::new(Ring::from_closed(shell), Vec::new()));
        let authority_plan = PreparedPlan::new(band.clone());
        let authority_cache = crate::geometry::FrameDependentCaches::default();
        let target = 15_u8;
        let new = cover(
            &band,
            &band,
            &authority_plan,
            &authority_cache,
            crate::geographic::GeographicFrame::crs84(),
            false,
            vec![tile::root()],
            target,
            None,
        )
        .expect("cover");
        let old = cover_recompute_baseline(&band, vec![tile::root()], target, None).expect("base");
        assert_eq!(
            new.iter().map(|(c, i)| (*c, *i)).collect::<Vec<_>>(),
            old.iter().map(|(c, i)| (*c, *i)).collect::<Vec<_>>(),
            "bounds-carry must emit identical (cell, interior) sets"
        );
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
            false,
            geohash::roots(),
            10,
            Some(crate::grid::GRID_MAX_CELLS),
        ) {
            Ok(_) => panic!("world at precision 10 must exceed the cell budget"),
            Err(CoverError::Budget(err)) => {
                assert_eq!(err.limit, crate::grid::GRID_MAX_CELLS);
                assert!(err.to_string().contains("max_cells"));
            },
            Err(CoverError::Preparation(err)) => panic!("unexpected preparation error: {err}"),
        }
    }

    #[test]
    fn cover_center_emits_only_center_covered_cells() {
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
        let center = cover_center(
            &poly,
            &poly,
            &authority_plan,
            &authority_cache,
            crate::geographic::GeographicFrame::crs84(),
            false,
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
            false,
            geohash::roots(),
            depth,
            None,
        )
        .expect("cover");
        let classifier = NativeRectClassifier::new(&poly).expect("classifier");
        let filtered: Vec<_> = overlap
            .into_iter()
            .filter_map(|(cell, interior)| {
                if interior {
                    return Some(cell);
                }
                let bounds = cell.bounds();
                let c = Point::new_raw_xy(
                    f64::midpoint(bounds.minx(), bounds.maxx()),
                    f64::midpoint(bounds.miny(), bounds.maxy()),
                );
                classifier.covers_point(c).then_some(cell)
            })
            .collect();
        assert_eq!(center, filtered);
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
            true,
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
            false,
            geohash::roots(),
            3,
            None,
        )
        .unwrap();
        assert_eq!(
            from_raw.iter().map(|(cell, _)| cell).collect::<Vec<_>>(),
            from_chart.iter().map(|(cell, _)| cell).collect::<Vec<_>>()
        );
    }
}
