#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
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
    Bounds, CoordSeq, Point, PointBatchTester, Polygon, Segment, SegmentIndex, Shape, Strictness,
    XY, line_segments,
};
use crate::grid::{CoverBudgetExceeded, ensure_cover_budget};

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
    fn children(self) -> impl Iterator<Item = Self>;
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

/// One canonical sorted target-depth cell set with its interior classifier.
pub(crate) struct RectCovering<C> {
    pub cells: Vec<(C, bool)>,
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

struct AreaRectClassifier {
    bounds: Option<Bounds>,
    tester: Arc<PointBatchTester>,
    boundary_index: SegmentIndex,
}

impl NativeRectClassifier {
    pub(crate) fn new(source: &Shape) -> Self {
        Self::new_with_area_tester(source, None)
    }

    /// Build with a reusable polygonal point tester when the caller already
    /// prepared one for the same areal source (S2 does this for vertex and
    /// center probes). Non-polygonal/mixed sources keep an independently
    /// prepared tester because their dissolved-area semantics differ.
    pub(crate) fn new_with_area_tester(
        source: &Shape,
        area_tester: Option<Arc<PointBatchTester>>,
    ) -> Self {
        let mut polygons = Vec::new();
        collect_area(source, &mut polygons);
        let area = dissolved_area_shape(polygons).map(|shape| {
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

        Self {
            source_bounds: source.bounds(),
            area,
            line_index,
            points: points.into_boxed_slice(),
        }
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
        let center = Point::new_unchecked_xy(
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
        _ => Some(Shape::MultiPolygon(polygons)),
    }
}

fn dissolved_area_shape(polygons: Vec<Polygon>) -> Option<Shape> {
    if polygons.len() <= 1 {
        return area_shape(polygons);
    }
    let parts: Vec<_> = polygons.into_iter().map(Shape::Polygon).collect();
    let dissolved = Shape::union_all(&parts, Strictness::Strict)
        .expect("polygonal covering union should not fail with ordinate dropping");
    let mut polygons = Vec::new();
    collect_area(&dissolved, &mut polygons);
    area_shape(polygons)
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
    #[expect(clippy::float_cmp)]
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
/// Interior branches bulk-emit their descendants; boundary branches descend;
/// empty branches prune. `roots` must be DISJOINT cells (every caller passes
/// the system's base cells), so each target-depth cell is classified exactly
/// once and the outputs need ordering but never dedup.
///
/// The emitted cell count is bounded by `max_cells` (when `Some`), checked at
/// every emission: a world-scale geometry at a fine `target_depth` fails with
/// [`CoverBudgetExceeded`] (naming `max_cells`) before the next allocation
/// instead of flooding memory. `None` = unlimited.
pub(crate) fn cover<C: RectCell>(
    source: &Shape,
    roots: Vec<C>,
    target_depth: u8,
    max_cells: Option<usize>,
) -> Result<RectCovering<C>, CoverBudgetExceeded> {
    let classifier = NativeRectClassifier::new(source);
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
                    // A boundary cell at the target depth intersects but is
                    // not fully covered.
                    cells.push((cell, false));
                    ensure_cover_budget(cells.len(), max_cells)?;
                } else {
                    stack.extend(cell.children().map(|child| (child, false)));
                }
            },
        }
    }
    cells.sort_unstable_by_key(|(cell, _)| *cell);
    Ok(RectCovering { cells })
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
    use crate::grid::geohash;

    fn contact(start: (f64, f64), end: (f64, f64)) -> RectContact {
        let unit = Bounds::new_unchecked(0.0, 0.0, 1.0, 1.0);
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

    /// A near-world polygon covered at a fine depth expands past the shared
    /// cell budget; the coverer fails deterministically (naming its depth
    /// parameter) during descendant emission rather than flooding memory.
    #[test]
    fn cover_rejects_world_scale_fine_depth_before_flooding() {
        use crate::geometry::Ring;
        let shell = vec![
            Point::new_unchecked_xy(-179.0, -85.0),
            Point::new_unchecked_xy(179.0, -85.0),
            Point::new_unchecked_xy(179.0, 85.0),
            Point::new_unchecked_xy(-179.0, 85.0),
            Point::new_unchecked_xy(-179.0, -85.0),
        ];
        let world = Shape::Polygon(Polygon::new(Ring::from_trusted_closed(shell), Vec::new()));
        // `RectCovering` is not `Debug`; match rather than `expect_err`.
        match cover(
            &world,
            geohash::roots(),
            10,
            Some(crate::grid::GRID_MAX_CELLS),
        ) {
            Ok(_) => panic!("world at precision 10 must exceed the cell budget"),
            Err(err) => {
                assert_eq!(err.limit, crate::grid::GRID_MAX_CELLS);
                assert!(err.to_string().contains("max_cells"));
            },
        }
    }
}
