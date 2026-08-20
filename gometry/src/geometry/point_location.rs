//! Hierarchical prepared point-in-polygon membership.
//!
//! Ownership mirrors geometry structure:
//! - [`PointBatchTester`] — multipolygon parts via [`YStabbingIndex`]
//! - [`PolygonPointIndex`] — shell ring + holes via [`YStabbingIndex`]
//! - [`RingPointIndex`] — ring edges via the same index type
//!
//! Every descent applies a full X/Y bounds check before exact classification.
//! Semantics are exact: polygon = shell minus its holes; multipolygon = any
//! Interior wins, else Boundary, else Exterior (never global XOR across parts).
//! Crossing arithmetic reuses [`ray_crossing_is_right`] / [`orientation_xy`].

use std::ops::ControlFlow;
use std::sync::OnceLock;

use crate::HeapSize;
use crate::geometry::segment_index::{EdgeId, EdgeYIndex, YStabbingIndex};
use crate::geometry::{
    Bounds, Orientation, Point, Polygon, RingClass, Shape, orientation_xy, ray_crossing_is_right,
    xy_bounds_columns,
};

/// One closed ring's Y-indexed edges for prepared point membership.
pub(crate) struct RingPointIndex {
    bounds: Bounds,
    xs: Box<[f64]>,
    ys: Box<[f64]>,
    /// Edge start indices (`0..n-1`) stabbed by Y — dense CSR stores edge
    /// ids directly (no items-vector indirection).
    edges: EdgeYIndex,
}

impl RingPointIndex {
    fn build(xs: &[f64], ys: &[f64]) -> Self {
        let n = xs.len();
        debug_assert_eq!(n, ys.len());
        let bounds = if n == 0 {
            Bounds::new_unchecked(
                f64::INFINITY,
                f64::INFINITY,
                f64::NEG_INFINITY,
                f64::NEG_INFINITY,
            )
        } else {
            let [minx, miny, maxx, maxy] = xy_bounds_columns(xs, ys);
            Bounds::new_unchecked(minx, miny, maxx, maxy)
        };
        Self {
            bounds,
            xs: xs.to_vec().into_boxed_slice(),
            ys: ys.to_vec().into_boxed_slice(),
            edges: EdgeYIndex::build(ys),
        }
    }

    fn bounds_contains(&self, x: f64, y: f64) -> bool {
        x >= self.bounds.minx()
            && x <= self.bounds.maxx()
            && y >= self.bounds.miny()
            && y <= self.bounds.maxy()
    }

    fn edge_xy(&self, edge: EdgeId) -> (f64, f64, f64, f64) {
        let e = edge.as_usize();
        debug_assert_eq!(self.xs.len(), self.ys.len());
        debug_assert!(e + 1 < self.xs.len());
        // SAFETY: EdgeYIndex::build creates exactly one id per adjacent Y pair,
        // and its dense/tree plans only emit those ids. The index is built from
        // these same columns, whose equal lengths make every emitted id satisfy
        // `e + 1 < self.xs.len()`.
        unsafe {
            (
                *self.xs.get_unchecked(e),
                *self.ys.get_unchecked(e),
                *self.xs.get_unchecked(e + 1),
                *self.ys.get_unchecked(e + 1),
            )
        }
    }

    /// Exact classification without the ring envelope gate.
    fn classify_at(&self, x: f64, y: f64) -> RingClass {
        if let Some(edges) = self.edges.dense_band_edges(y) {
            let mut inside = false;
            for &edge in edges {
                let (ax, ay, bx, by) = self.edge_xy(edge);
                let in_bbox =
                    x >= ax.min(bx) && x <= ax.max(bx) && y >= ay.min(by) && y <= ay.max(by);
                let orientation = in_bbox.then(|| orientation_xy(ax, ay, bx, by, x, y));
                if orientation == Some(Orientation::Collinear) {
                    return RingClass::Boundary;
                }
                if (ay > y) != (by > y)
                    && orientation.map_or_else(
                        || ray_crossing_is_right(ax, ay, bx, by, x, y),
                        |orientation| (orientation == Orientation::CounterClockwise) == (by > ay),
                    )
                {
                    inside = !inside;
                }
            }
            return if inside {
                RingClass::Interior
            } else {
                RingClass::Exterior
            };
        }

        let mut inside = false;
        if self.edges.for_each_edge(y, |edge| {
            let (ax, ay, bx, by) = self.edge_xy(edge);
            let in_bbox = x >= ax.min(bx) && x <= ax.max(bx) && y >= ay.min(by) && y <= ay.max(by);
            let orientation = in_bbox.then(|| orientation_xy(ax, ay, bx, by, x, y));
            if orientation == Some(Orientation::Collinear) {
                return ControlFlow::Break(RingClass::Boundary);
            }
            if (ay > y) != (by > y)
                && orientation.map_or_else(
                    || ray_crossing_is_right(ax, ay, bx, by, x, y),
                    |orientation| (orientation == Orientation::CounterClockwise) == (by > ay),
                )
            {
                inside = !inside;
            }
            ControlFlow::Continue(())
        }) == Some(RingClass::Boundary)
        {
            return RingClass::Boundary;
        }
        if inside {
            RingClass::Interior
        } else {
            RingClass::Exterior
        }
    }
}

impl HeapSize for RingPointIndex {
    fn heap_bytes(&self) -> usize {
        self.xs.heap_bytes() + self.ys.heap_bytes() + self.edges.heap_bytes()
    }
}

/// One polygon: shell minus holes, with hierarchical Y-stabbing on both levels.
pub(crate) struct PolygonPointIndex {
    bounds: Option<Bounds>,
    shell: RingPointIndex,
    holes: YStabbingIndex<RingPointIndex>,
}

const CELL_GRID_SIDE: usize = 64;
const CELL_GRID_LEN: usize = CELL_GRID_SIDE * CELL_GRID_SIDE;
const CELL_USE_MIN_PROBES: usize = 10_000;

const fn cell_build_min_probes(total_edges: usize) -> usize {
    4_096_usize.saturating_add(total_edges.saturating_mul(2))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CellPreclassValue {
    Outside,
    Inside,
    Maybe,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CellCertificate {
    Interior,
    Exterior,
}

impl HeapSize for CellPreclassValue {
    fn heap_bytes(&self) -> usize {
        0
    }
}

struct CellPreclass {
    bounds: Bounds,
    x_boundaries: [f64; CELL_GRID_SIDE + 1],
    y_boundaries: [f64; CELL_GRID_SIDE + 1],
    cells: Box<[CellPreclassValue; CELL_GRID_LEN]>,
}

impl HeapSize for CellPreclass {
    fn heap_bytes(&self) -> usize {
        self.cells.heap_bytes()
    }
}

impl CellPreclass {
    fn axis_boundaries(min: f64, max: f64) -> [f64; CELL_GRID_SIDE + 1] {
        let step = (max - min) / CELL_GRID_SIDE as f64;
        std::array::from_fn(|line| {
            if line == CELL_GRID_SIDE {
                max
            } else {
                min + line as f64 * step
            }
        })
    }

    fn axis_cell(boundaries: &[f64; CELL_GRID_SIDE + 1], coordinate: f64) -> usize {
        boundaries[1..]
            .partition_point(|&boundary| boundary <= coordinate)
            .min(CELL_GRID_SIDE - 1)
    }

    fn cell_x(&self, x: f64) -> usize {
        Self::axis_cell(&self.x_boundaries, x)
    }

    fn cell_y(&self, y: f64) -> usize {
        Self::axis_cell(&self.y_boundaries, y)
    }

    const fn x_boundary(&self, column: usize) -> f64 {
        self.x_boundaries[column]
    }

    const fn y_boundary(&self, row: usize) -> f64 {
        self.y_boundaries[row]
    }

    fn lookup(&self, point: Point) -> CellPreclassValue {
        if point.x < self.bounds.minx()
            || point.x > self.bounds.maxx()
            || point.y < self.bounds.miny()
            || point.y > self.bounds.maxy()
        {
            return CellPreclassValue::Outside;
        }
        self.cells[self.cell_y(point.y) * CELL_GRID_SIDE + self.cell_x(point.x)]
    }

    fn build(tester: &PointBatchTester) -> Option<Self> {
        let bounds = tester.polygon_bounds()?;
        let width = bounds.maxx() - bounds.minx();
        let height = bounds.maxy() - bounds.miny();
        if !(width.is_finite() && height.is_finite() && width > 0.0 && height > 0.0) {
            return None;
        }
        let mut grid = Self {
            bounds,
            x_boundaries: Self::axis_boundaries(bounds.minx(), bounds.maxx()),
            y_boundaries: Self::axis_boundaries(bounds.miny(), bounds.maxy()),
            cells: Box::new([CellPreclassValue::Outside; CELL_GRID_LEN]),
        };
        tester.for_each_ring(|xs, ys| grid.mark_ring(xs, ys));

        let mut visited = [false; CELL_GRID_LEN];
        let mut work = vec![0_usize; CELL_GRID_LEN];
        for seed in 0..CELL_GRID_LEN {
            if visited[seed] || grid.cells[seed] == CellPreclassValue::Maybe {
                continue;
            }
            let mut head = 0;
            let mut tail = 1;
            work[0] = seed;
            visited[seed] = true;
            let representative = grid.cell_center(seed);
            while head < tail {
                let cell = work[head];
                head += 1;
                let row = cell / CELL_GRID_SIDE;
                let col = cell % CELL_GRID_SIDE;
                for (next, valid) in [
                    (row.checked_sub(1).map(|r| r * CELL_GRID_SIDE + col), true),
                    (
                        (row + 1 < CELL_GRID_SIDE).then_some((row + 1) * CELL_GRID_SIDE + col),
                        true,
                    ),
                    (col.checked_sub(1).map(|c| row * CELL_GRID_SIDE + c), true),
                    (
                        (col + 1 < CELL_GRID_SIDE).then_some(row * CELL_GRID_SIDE + col + 1),
                        true,
                    ),
                ] {
                    let Some(next) = next else { continue };
                    if valid && !visited[next] && grid.cells[next] != CellPreclassValue::Maybe {
                        visited[next] = true;
                        work[tail] = next;
                        tail += 1;
                    }
                }
            }
            let value = match tester.classify_area_point(representative) {
                Some(RingClass::Interior) => CellPreclassValue::Inside,
                Some(RingClass::Exterior) => CellPreclassValue::Outside,
                Some(RingClass::Boundary) | None => CellPreclassValue::Maybe,
            };
            for &cell in &work[..tail] {
                grid.cells[cell] = value;
            }
        }
        Some(grid)
    }

    fn cell_center(&self, cell: usize) -> Point {
        let row = cell / CELL_GRID_SIDE;
        let col = cell % CELL_GRID_SIDE;
        let x0 = self.x_boundary(col);
        let y0 = self.y_boundary(row);
        Point::new_unchecked_xy(
            x0 + (self.x_boundary(col + 1) - x0) * 0.5,
            y0 + (self.y_boundary(row + 1) - y0) * 0.5,
        )
    }

    fn mark(&mut self, row: usize, col: usize) {
        self.cells[row * CELL_GRID_SIDE + col] = CellPreclassValue::Maybe;
    }

    fn axis_range(
        boundaries: &[f64; CELL_GRID_SIDE + 1],
        min: f64,
        max: f64,
    ) -> Option<(usize, usize)> {
        if !min.is_finite() || !max.is_finite() || min > max {
            return None;
        }
        let first = boundaries.partition_point(|&boundary| boundary < min);
        let last = boundaries.partition_point(|&boundary| boundary <= max);
        Some((
            first.saturating_sub(1),
            last.saturating_sub(1).min(CELL_GRID_SIDE - 1),
        ))
    }

    fn mark_endpoint(&mut self, x: f64, y: f64) {
        for row in 0..CELL_GRID_SIDE {
            if self.y_boundary(row) <= y && y <= self.y_boundary(row + 1) {
                for col in 0..CELL_GRID_SIDE {
                    if self.x_boundary(col) <= x && x <= self.x_boundary(col + 1) {
                        self.mark(row, col);
                    }
                }
            }
        }
    }

    fn mark_ring(&mut self, xs: &[f64], ys: &[f64]) {
        for (x, y, nx, ny) in xs
            .iter()
            .zip(ys)
            .zip(xs.iter().skip(1).zip(ys.iter().skip(1)))
            .map(|((x, y), (nx, ny))| (*x, *y, *nx, *ny))
        {
            self.mark_segment(x, y, nx, ny);
        }
    }

    fn mark_segment(&mut self, ax: f64, ay: f64, bx: f64, by: f64) {
        if ![ax, ay, bx, by].into_iter().all(f64::is_finite) {
            self.cells.fill(CellPreclassValue::Maybe);
            return;
        }
        self.mark_endpoint(ax, ay);
        self.mark_endpoint(bx, by);
        let dx = bx - ax;
        let dy = by - ay;
        if dx == 0.0 {
            let min_y = ay.min(by);
            let max_y = ay.max(by);
            for col in 0..CELL_GRID_SIDE {
                if self.x_boundary(col) <= ax && ax <= self.x_boundary(col + 1) {
                    for row in 0..CELL_GRID_SIDE {
                        if self.y_boundary(row) <= max_y && min_y <= self.y_boundary(row + 1) {
                            self.mark(row, col);
                        }
                    }
                }
            }
        } else if dy == 0.0 {
            let min_x = ax.min(bx);
            let max_x = ax.max(bx);
            for row in 0..CELL_GRID_SIDE {
                if self.y_boundary(row) <= ay && ay <= self.y_boundary(row + 1) {
                    for col in 0..CELL_GRID_SIDE {
                        if self.x_boundary(col) <= max_x && min_x <= self.x_boundary(col + 1) {
                            self.mark(row, col);
                        }
                    }
                }
            }
        } else {
            let min_x = ax.min(bx);
            let max_x = ax.max(bx);
            let min_y = ay.min(by);
            let max_y = ay.max(by);

            for colline in 1..CELL_GRID_SIDE {
                let x = self.x_boundary(colline);
                if x < min_x || x > max_x {
                    continue;
                }
                let y = ay + (by - ay) * ((x - ax) / (bx - ax));
                if !y.is_finite() {
                    self.cells.fill(CellPreclassValue::Maybe);
                    return;
                }
                let y_min = y.next_down().next_down();
                let y_max = y.next_up().next_up();
                let Some((first, last)) = Self::axis_range(&self.y_boundaries, y_min, y_max) else {
                    self.cells.fill(CellPreclassValue::Maybe);
                    return;
                };
                for row in first..=last {
                    self.mark(row, colline - 1);
                    self.mark(row, colline);
                }
            }

            for rowline in 1..CELL_GRID_SIDE {
                let y = self.y_boundary(rowline);
                if y < min_y || y > max_y {
                    continue;
                }
                let x = ax + (bx - ax) * ((y - ay) / (by - ay));
                if !x.is_finite() {
                    self.cells.fill(CellPreclassValue::Maybe);
                    return;
                }
                let x_min = x.next_down().next_down();
                let x_max = x.next_up().next_up();
                let Some((first, last)) = Self::axis_range(&self.x_boundaries, x_min, x_max) else {
                    self.cells.fill(CellPreclassValue::Maybe);
                    return;
                };
                for col in first..=last {
                    self.mark(rowline - 1, col);
                    self.mark(rowline, col);
                }
            }
        }
    }
}

impl PolygonPointIndex {
    fn build(polygon: &Polygon) -> Self {
        let shell = RingPointIndex::build(polygon.shell.coords().xs(), polygon.shell.coords().ys());
        let hole_rings: Vec<RingPointIndex> = polygon
            .holes
            .iter()
            .map(|hole| RingPointIndex::build(hole.coords().xs(), hole.coords().ys()))
            .collect();
        let holes =
            YStabbingIndex::build(hole_rings, |ring| (ring.bounds.miny(), ring.bounds.maxy()));
        Self {
            bounds: Bounds::from_coords(polygon.shell.coords()),
            shell,
            holes,
        }
    }

    fn in_bounds_xy(&self, x: f64, y: f64) -> bool {
        self.bounds.is_some_and(|bounds| {
            x >= bounds.minx() && x <= bounds.maxx() && y >= bounds.miny() && y <= bounds.maxy()
        })
    }

    fn contains_xy(&self, x: f64, y: f64) -> bool {
        if !self.in_bounds_xy(x, y) {
            return false;
        }
        // Shell envelope ≈ polygon envelope — skip redundant ring gate.
        if self.shell.classify_at(x, y) != RingClass::Interior {
            return false;
        }
        !self.hole_hits_xy(x, y, |class| class != RingClass::Exterior)
    }

    fn covers_xy(&self, x: f64, y: f64) -> bool {
        if !self.in_bounds_xy(x, y) {
            return false;
        }
        if self.shell.classify_at(x, y) == RingClass::Exterior {
            return false;
        }
        !self.hole_hits_xy(x, y, |class| class == RingClass::Interior)
    }

    /// Three-way membership: `Boundary` on shell or any hole ring, `Interior`
    /// if strictly inside the shell and strictly outside every hole, else
    /// `Exterior`. Bounds-gate only rules out `Exterior`.
    fn classify_xy(&self, x: f64, y: f64) -> RingClass {
        if !self.in_bounds_xy(x, y) {
            return RingClass::Exterior;
        }
        match self.shell.classify_at(x, y) {
            RingClass::Exterior => RingClass::Exterior,
            RingClass::Boundary => RingClass::Boundary,
            RingClass::Interior => self.classify_holes_xy(x, y),
        }
    }

    /// Shell already classified Interior; fold hole rings.
    fn classify_holes_xy(&self, x: f64, y: f64) -> RingClass {
        let mut on_hole = false;
        let mut interior_hole = false;
        // Few holes (common): full XY envelope gate per hole without
        // Y-index indirection. Many holes: Y-stab then XY gate.
        if self.holes.len() <= 32 {
            self.holes.for_each(|hole| {
                if interior_hole || !hole.bounds_contains(x, y) {
                    return;
                }
                match hole.classify_at(x, y) {
                    RingClass::Interior => interior_hole = true,
                    RingClass::Boundary => on_hole = true,
                    RingClass::Exterior => {},
                }
            });
        } else {
            let _ = self.holes.for_each_at_y(y, |hole| {
                if interior_hole {
                    return ControlFlow::Break(());
                }
                if !hole.bounds_contains(x, y) {
                    return ControlFlow::Continue(());
                }
                match hole.classify_at(x, y) {
                    RingClass::Interior => {
                        interior_hole = true;
                        ControlFlow::Break(())
                    },
                    RingClass::Boundary => {
                        on_hole = true;
                        ControlFlow::Continue(())
                    },
                    RingClass::Exterior => ControlFlow::Continue(()),
                }
            });
        }
        if interior_hole {
            RingClass::Exterior
        } else if on_hole {
            RingClass::Boundary
        } else {
            RingClass::Interior
        }
    }

    /// True if any hole (after XY bounds gate) satisfies `pred`.
    fn hole_hits_xy(&self, x: f64, y: f64, pred: impl Fn(RingClass) -> bool) -> bool {
        let mut hit = false;
        if self.holes.len() <= 32 {
            self.holes.for_each(|hole| {
                if hit || !hole.bounds_contains(x, y) {
                    return;
                }
                if pred(hole.classify_at(x, y)) {
                    hit = true;
                }
            });
        } else {
            let _ = self.holes.for_each_at_y(y, |hole| {
                if hit {
                    return ControlFlow::Break(());
                }
                if !hole.bounds_contains(x, y) {
                    return ControlFlow::Continue(());
                }
                if pred(hole.classify_at(x, y)) {
                    hit = true;
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                }
            });
        }
        hit
    }
}

impl HeapSize for PolygonPointIndex {
    fn heap_bytes(&self) -> usize {
        self.shell.heap_bytes() + self.holes.heap_bytes()
    }
}

/// Batched point-membership tester for a fixed shape: polygonal shapes get a
/// hierarchical Y-stabbing plan (parts → shell/holes → edges); everything else
/// falls through to the point kernels.
#[expect(
    private_interfaces,
    reason = "the grid payload remains private to point_location"
)]
pub(crate) enum PointBatchTester {
    /// Single polygon — no multipolygon Y-index indirection.
    Polygon {
        index: PolygonPointIndex,
        total_edges: usize,
        cell_preclass: OnceLock<CellPreclass>,
    },
    /// Multiple parts stabbed by Y; each part is an independent polygon
    /// (union semantics: any Interior wins).
    MultiPolygon {
        parts: YStabbingIndex<PolygonPointIndex>,
        total_edges: usize,
        cell_preclass: OnceLock<CellPreclass>,
    },
    Generic(Shape),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PointProbeUse {
    OneShot(usize),
    AcrossCalls,
}

impl PointProbeUse {
    pub(crate) const fn for_plan(self, probes: usize) -> Self {
        match self {
            Self::OneShot(_) => Self::OneShot(probes),
            Self::AcrossCalls => Self::AcrossCalls,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PointPlanSummary {
    total_edges: usize,
    has_indexed_stage: bool,
}

fn point_plan_summary(shape: &Shape) -> Option<PointPlanSummary> {
    use crate::geometry::segment_index::uses_linear_plan_for_len;

    fn add_polygon(polygon: &Polygon, total_edges: &mut usize, has_indexed_stage: &mut bool) {
        let shell_edges = polygon.shell.coords().len().saturating_sub(1);
        *total_edges = total_edges.saturating_add(shell_edges);
        *has_indexed_stage |= !uses_linear_plan_for_len(shell_edges);
        for hole in polygon.holes.iter() {
            let edges = hole.coords().len().saturating_sub(1);
            *total_edges = total_edges.saturating_add(edges);
            *has_indexed_stage |= !uses_linear_plan_for_len(edges);
        }
        *has_indexed_stage |= !uses_linear_plan_for_len(polygon.holes.len());
    }
    let mut total_edges = 0_usize;
    let mut has_indexed_stage = false;
    match shape {
        Shape::Polygon(polygon) => add_polygon(polygon, &mut total_edges, &mut has_indexed_stage),
        Shape::MultiPolygon(polygons) => {
            has_indexed_stage |= !uses_linear_plan_for_len(polygons.len());
            for polygon in polygons {
                add_polygon(polygon, &mut total_edges, &mut has_indexed_stage);
            }
        },
        _ => return None,
    }
    Some(PointPlanSummary {
        total_edges,
        has_indexed_stage,
    })
}

const fn one_shot_crossover(edges: usize) -> usize {
    let e = edges as u128;
    ((38 * e + 4_500).div_ceil(10 * e)) as usize
}

impl PointBatchTester {
    #[cfg(test)]
    const fn uses_indexed_plan(&self) -> bool {
        match self {
            Self::Polygon { index: polygon, .. } => {
                polygon.shell.edges.uses_indexed_plan() || polygon.holes.uses_indexed_plan()
            },
            Self::MultiPolygon { parts, .. } => parts.uses_indexed_plan(),
            Self::Generic(_) => false,
        }
    }

    pub(crate) fn should_use(shape: &Shape, use_mode: PointProbeUse) -> bool {
        let Some(summary) = point_plan_summary(shape) else {
            return false;
        };
        summary.has_indexed_stage
            && match use_mode {
                PointProbeUse::AcrossCalls => true,
                PointProbeUse::OneShot(probes) => probes >= one_shot_crossover(summary.total_edges),
            }
    }

    pub(crate) fn new(shape: &Shape) -> Self {
        match shape {
            Shape::Polygon(polygon) => Self::Polygon {
                index: PolygonPointIndex::build(polygon),
                total_edges: polygon.shell.coords().len().saturating_sub(1)
                    + polygon
                        .holes
                        .iter()
                        .map(|h| h.coords().len().saturating_sub(1))
                        .sum::<usize>(),
                cell_preclass: OnceLock::new(),
            },
            Shape::MultiPolygon(polygons) if polygons.len() == 1 => Self::Polygon {
                index: PolygonPointIndex::build(&polygons[0]),
                total_edges: polygons[0].shell.coords().len().saturating_sub(1)
                    + polygons[0]
                        .holes
                        .iter()
                        .map(|h| h.coords().len().saturating_sub(1))
                        .sum::<usize>(),
                cell_preclass: OnceLock::new(),
            },
            Shape::MultiPolygon(polygons) => {
                let built: Vec<PolygonPointIndex> =
                    polygons.iter().map(PolygonPointIndex::build).collect();
                let parts = YStabbingIndex::build(built, |p| {
                    p.bounds
                        .map_or((f64::INFINITY, f64::NEG_INFINITY), |b| (b.miny(), b.maxy()))
                });
                Self::MultiPolygon {
                    parts,
                    total_edges: polygons
                        .iter()
                        .map(|p| {
                            p.shell.coords().len().saturating_sub(1)
                                + p.holes
                                    .iter()
                                    .map(|h| h.coords().len().saturating_sub(1))
                                    .sum::<usize>()
                        })
                        .sum(),
                    cell_preclass: OnceLock::new(),
                }
            },
            _ => Self::Generic(shape.clone()),
        }
    }

    fn polygon_bounds(&self) -> Option<Bounds> {
        match self {
            Self::Polygon { index, .. } => index.bounds,
            Self::MultiPolygon { parts, .. } => {
                let mut result = None;
                parts.for_each(|part| {
                    if let Some(bounds) = part.bounds {
                        result = Some(result.map_or(bounds, |mut current: Bounds| {
                            current.include_bounds(bounds);
                            current
                        }));
                    }
                });
                result
            },
            Self::Generic(_) => None,
        }
    }

    fn for_each_ring(&self, mut visit: impl FnMut(&[f64], &[f64])) {
        let mut polygon = |polygon: &PolygonPointIndex| {
            visit(&polygon.shell.xs, &polygon.shell.ys);
            polygon.holes.for_each(|hole| visit(&hole.xs, &hole.ys));
        };
        match self {
            Self::Polygon { index, .. } => polygon(index),
            Self::MultiPolygon { parts, .. } => parts.for_each(polygon),
            Self::Generic(_) => {},
        }
    }

    fn cell_preclass_for(&self, probes: usize) -> Option<&CellPreclass> {
        if probes < CELL_USE_MIN_PROBES {
            return None;
        }
        let (lock, total_edges) = match self {
            Self::Polygon {
                cell_preclass,
                total_edges,
                ..
            }
            | Self::MultiPolygon {
                cell_preclass,
                total_edges,
                ..
            } => (cell_preclass, *total_edges),
            Self::Generic(_) => return None,
        };
        if let Some(grid) = lock.get() {
            return Some(grid);
        }
        let bounds = self.polygon_bounds()?;
        let width = bounds.maxx() - bounds.minx();
        let height = bounds.maxy() - bounds.miny();
        if probes < cell_build_min_probes(total_edges)
            || !(width.is_finite() && height.is_finite() && width > 0.0 && height > 0.0)
        {
            return None;
        }
        lock.get_or_init(|| CellPreclass::build(self).expect("admitted finite polygon bounds"));
        lock.get()
    }

    fn cell_classify(&self, grid: &CellPreclass, point: Point) -> Option<CellCertificate> {
        match grid.lookup(point) {
            CellPreclassValue::Inside => Some(CellCertificate::Interior),
            CellPreclassValue::Outside => Some(CellCertificate::Exterior),
            CellPreclassValue::Maybe => None,
        }
    }

    pub(crate) fn cell_batch_classify(
        &self,
        probes: usize,
        points: &[Point],
    ) -> Option<Vec<Option<CellCertificate>>> {
        let grid = self.cell_preclass_for(probes)?;
        Some(
            points
                .iter()
                .map(|&point| self.cell_classify(grid, point))
                .collect(),
        )
    }

    /// Strict membership — [`Shape::contains_point`] semantics.
    pub(crate) fn contains_point(&self, point: Point) -> bool {
        match self {
            Self::Polygon { index: polygon, .. } => polygon.contains_xy(point.x, point.y),
            Self::MultiPolygon { parts, .. } => {
                let mut hit = false;
                let _ = parts.for_each_at_y(point.y, |polygon| {
                    if polygon.contains_xy(point.x, point.y) {
                        hit = true;
                        ControlFlow::Break(())
                    } else {
                        ControlFlow::Continue(())
                    }
                });
                hit
            },
            Self::Generic(shape) => shape.contains_point(point),
        }
    }

    /// Boundary-inclusive membership — [`Shape::covers_point`] semantics.
    pub(crate) fn covers_point(&self, point: Point) -> bool {
        match self {
            Self::Polygon { index: polygon, .. } => polygon.covers_xy(point.x, point.y),
            Self::MultiPolygon { parts, .. } => {
                let mut hit = false;
                let _ = parts.for_each_at_y(point.y, |polygon| {
                    if polygon.covers_xy(point.x, point.y) {
                        hit = true;
                        ControlFlow::Break(())
                    } else {
                        ControlFlow::Continue(())
                    }
                });
                hit
            },
            Self::Generic(shape) => shape.covers_point(point),
        }
    }

    /// Three-way areal membership: `Interior` (strictly inside the area),
    /// `Boundary` (on a ring), or `Exterior`. Strictly interior to ANY part
    /// wins over a boundary hit on another (union semantics).
    pub(crate) fn classify_area_point(&self, point: Point) -> Option<RingClass> {
        match self {
            Self::Polygon { index: polygon, .. } => Some(polygon.classify_xy(point.x, point.y)),
            Self::MultiPolygon { parts, .. } => {
                let mut on_boundary = false;
                let mut interior = false;
                let _ = parts.for_each_at_y(point.y, |polygon| {
                    if interior {
                        return ControlFlow::Break(());
                    }
                    match polygon.classify_xy(point.x, point.y) {
                        RingClass::Interior => {
                            interior = true;
                            ControlFlow::Break(())
                        },
                        RingClass::Boundary => {
                            on_boundary = true;
                            ControlFlow::Continue(())
                        },
                        RingClass::Exterior => ControlFlow::Continue(()),
                    }
                });
                Some(if interior {
                    RingClass::Interior
                } else if on_boundary {
                    RingClass::Boundary
                } else {
                    RingClass::Exterior
                })
            },
            Self::Generic(_) => None,
        }
    }
}

impl HeapSize for PointBatchTester {
    fn heap_bytes(&self) -> usize {
        match self {
            Self::Polygon {
                index: polygon,
                cell_preclass,
                ..
            } => polygon.heap_bytes() + cell_preclass.heap_bytes(),
            Self::MultiPolygon {
                parts,
                cell_preclass,
                ..
            } => parts.heap_bytes() + cell_preclass.heap_bytes(),
            Self::Generic(shape) => shape.coordinate_bytes(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::geometry::{XY, ring_classify_point};
    use crate::{CoordSeq, Ring};

    fn closed_ring(pts: &[(f64, f64)]) -> Ring {
        let xy: Vec<XY> = pts.iter().map(|&(x, y)| XY::new(x, y)).collect();
        Ring::from_trusted_closed(CoordSeq::from_xy(&xy))
    }

    fn unit_square() -> Polygon {
        Polygon {
            shell: closed_ring(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]),
            holes: Arc::from([]),
        }
    }

    fn regular_polygon(edge_count: usize) -> Shape {
        let mut points = Vec::with_capacity(edge_count + 1);
        for i in 0..edge_count {
            let angle = std::f64::consts::TAU * i as f64 / edge_count as f64;
            points.push(XY::new(angle.cos(), angle.sin()));
        }
        points.push(points[0]);
        Shape::Polygon(Polygon {
            shell: Ring::from_trusted_closed(CoordSeq::from_xy(&points)),
            holes: Arc::from([]),
        })
    }

    #[test]
    fn point_probe_policy_uses_index_plan_and_crossover() {
        assert!(!PointBatchTester::should_use(
            &Shape::Polygon(unit_square()),
            PointProbeUse::AcrossCalls,
        ));
        let polygon = regular_polygon(33);
        assert!(PointBatchTester::should_use(
            &polygon,
            PointProbeUse::AcrossCalls,
        ));
        assert!(!PointBatchTester::should_use(
            &polygon,
            PointProbeUse::OneShot(17)
        ));
        assert!(PointBatchTester::should_use(
            &polygon,
            PointProbeUse::OneShot(18)
        ));
        assert_eq!(one_shot_crossover(64), 11);
        assert_eq!(one_shot_crossover(256), 6);
        assert_eq!(one_shot_crossover(1_316), 5);
        assert_eq!(one_shot_crossover(10_000), 4);
    }

    #[test]
    fn point_probe_policy_indexes_polygon_hierarchy() {
        let mut polygon = unit_square();
        polygon.holes = Arc::from(
            std::iter::repeat_with(|| {
                closed_ring(&[(2.0, 2.0), (3.0, 2.0), (3.0, 3.0), (2.0, 3.0), (2.0, 2.0)])
            })
            .take(33)
            .collect::<Vec<_>>(),
        );
        let tester = PointBatchTester::new(&Shape::Polygon(polygon));
        assert!(tester.uses_indexed_plan());
    }

    #[test]
    fn point_probe_policy_indexes_33_multipolygon_parts() {
        let parts = (0..33)
            .map(|index| {
                let x = f64::from(index) * 2.0;
                Polygon {
                    shell: closed_ring(&[
                        (x, 0.0),
                        (x + 1.0, 0.0),
                        (x + 1.0, 1.0),
                        (x, 1.0),
                        (x, 0.0),
                    ]),
                    holes: Arc::from([]),
                }
            })
            .collect::<Vec<_>>();
        let tester = PointBatchTester::new(&Shape::MultiPolygon(parts));
        assert!(tester.uses_indexed_plan());
    }

    #[test]
    fn dense_ring_classification_matches_shape_kernel() {
        let mut points = Vec::with_capacity(37);
        points.extend((0..=9).map(|x| XY::new(f64::from(x), 0.0)));
        points.extend((1..=9).map(|y| XY::new(10.0, f64::from(y))));
        points.extend((0..=8).rev().map(|x| XY::new(f64::from(x), 10.0)));
        points.extend((1..=8).rev().map(|y| XY::new(0.0, f64::from(y))));
        points.push(XY::new(0.0, 0.0));
        let coords = CoordSeq::from(points);
        assert_eq!(coords.xs().len() - 1, 36);
        let ring = RingPointIndex::build(coords.xs(), coords.ys());
        assert!(ring.edges.dense_band_edges(5.0).is_some());

        for (x, y) in [(5.0, 0.0), (5.0, 5.0), (9.0, 5.0), (-1.0, 5.0)] {
            assert_eq!(
                ring.classify_at(x, y),
                ring_classify_point(&coords, XY::new(x, y).point()),
                "classification mismatch at ({x}, {y})"
            );
        }
    }

    fn square_with_hole() -> Polygon {
        Polygon {
            shell: closed_ring(&[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ]),
            holes: Arc::from([closed_ring(&[
                (3.0, 3.0),
                (7.0, 3.0),
                (7.0, 7.0),
                (3.0, 7.0),
                (3.0, 3.0),
            ])]),
        }
    }

    #[test]
    fn contains_covers_and_classify_match_shape_kernels() {
        let poly = square_with_hole();
        let shape = Shape::Polygon(poly);
        let tester = PointBatchTester::new(&shape);
        let probes = [
            (5.0, 5.0),
            (1.0, 1.0),
            (0.0, 5.0),
            (3.0, 5.0),
            (20.0, 20.0),
            (5.0, 1.0),
        ];
        for &(x, y) in &probes {
            let p = Point::new_unchecked_xy(x, y);
            assert_eq!(
                tester.contains_point(p),
                shape.contains_point(p),
                "contains at ({x},{y})"
            );
            assert_eq!(
                tester.covers_point(p),
                shape.covers_point(p),
                "covers at ({x},{y})"
            );
            let class = tester.classify_area_point(p).unwrap();
            let expect = if shape.contains_point(p) {
                RingClass::Interior
            } else if shape.covers_point(p) {
                RingClass::Boundary
            } else {
                RingClass::Exterior
            };
            assert_eq!(class, expect, "classify at ({x},{y})");
        }
    }

    #[test]
    fn multipolygon_is_union_not_xor() {
        let a = unit_square();
        let b = Polygon {
            shell: closed_ring(&[(0.5, 0.5), (1.5, 0.5), (1.5, 1.5), (0.5, 1.5), (0.5, 0.5)]),
            holes: Arc::from([]),
        };
        let shape = Shape::MultiPolygon(vec![a, b]);
        let tester = PointBatchTester::new(&shape);
        let overlap = Point::new_unchecked_xy(0.75, 0.75);
        assert!(tester.contains_point(overlap));
        assert_eq!(
            tester.classify_area_point(overlap),
            Some(RingClass::Interior)
        );
        let out = Point::new_unchecked_xy(3.0, 3.0);
        assert!(!tester.contains_point(out));
        assert_eq!(tester.classify_area_point(out), Some(RingClass::Exterior));
    }

    #[test]
    fn hole_envelope_skips_non_covering_holes() {
        let poly = square_with_hole();
        let tester = PointBatchTester::new(&Shape::Polygon(poly));
        let p = Point::new_unchecked_xy(1.0, 9.0);
        assert!(tester.contains_point(p));
        assert_eq!(tester.classify_area_point(p), Some(RingClass::Interior));
    }

    #[test]
    fn tall_edge_ring_constructs_and_classifies() {
        let n_side = 2000_usize;
        let mut pts = Vec::with_capacity(2 * n_side + 3);
        for i in 0..=n_side {
            pts.push(XY::new(0.0, i as f64));
        }
        pts.push(XY::new(1.0, n_side as f64));
        for i in (0..n_side).rev() {
            pts.push(XY::new(1.0, i as f64));
        }
        pts.push(XY::new(0.0, 0.0));
        let poly = Polygon {
            shell: Ring::from_trusted_closed(CoordSeq::from_xy(&pts)),
            holes: Arc::from([]),
        };
        let shape = Shape::Polygon(poly);
        let tester = PointBatchTester::new(&shape);
        assert!(tester.contains_point(Point::new_unchecked_xy(0.5, n_side as f64 * 0.5)));
        assert!(!tester.contains_point(Point::new_unchecked_xy(-1.0, n_side as f64 * 0.5)));
        let bytes = tester.heap_bytes();
        let verts = pts.len();
        assert!(
            bytes < verts * 128,
            "tall-edge prepared index not proportional: {bytes} bytes for {verts} verts"
        );
    }

    #[test]
    fn many_sparse_holes_match_shape() {
        let shell = closed_ring(&[
            (0.0, 0.0),
            (1000.0, 0.0),
            (1000.0, 1000.0),
            (0.0, 1000.0),
            (0.0, 0.0),
        ]);
        let mut holes = Vec::new();
        for i in 0..100_i32 {
            let cx = 50.0 + f64::from(i % 10) * 90.0;
            let cy = 50.0 + f64::from(i / 10) * 90.0;
            let r = 10.0;
            holes.push(closed_ring(&[
                (cx - r, cy - r),
                (cx + r, cy - r),
                (cx + r, cy + r),
                (cx - r, cy + r),
                (cx - r, cy - r),
            ]));
        }
        let poly = Polygon {
            shell,
            holes: Arc::from(holes),
        };
        let shape = Shape::Polygon(poly);
        let tester = PointBatchTester::new(&shape);
        for i in 0..200_i32 {
            let x = f64::from(i * 37 + 13) % 1000.0;
            let y = f64::from(i * 53 + 7) % 1000.0;
            let p = Point::new_unchecked_xy(x, y);
            assert_eq!(
                tester.contains_point(p),
                shape.contains_point(p),
                "mismatch at ({x},{y})"
            );
            assert_eq!(
                tester.covers_point(p),
                shape.covers_point(p),
                "covers mismatch at ({x},{y})"
            );
        }
    }

    #[test]
    fn cell_grid_has_separate_warm_and_cold_gates() {
        let shape = regular_polygon(64);
        let tester = PointBatchTester::new(&shape);
        assert!(tester.cell_preclass_for(9_999).is_none());
        assert!(match &tester {
            PointBatchTester::Polygon { cell_preclass, .. } => cell_preclass.get().is_none(),
            _ => false,
        });
        let grid = tester
            .cell_preclass_for(10_000)
            .expect("64-edge grid admits at 10k");
        assert!(tester.cell_preclass_for(9_999).is_none());
        for point in [
            Point::new_unchecked_xy(0.0, 0.0),
            Point::new_unchecked_xy(0.5, 0.5),
            Point::new_unchecked_xy(2.0, 0.0),
            Point::new_unchecked_xy(-1.0, 0.0),
        ] {
            let value = grid.lookup(point);
            let exact = tester.classify_area_point(point).unwrap();
            assert!(match exact {
                RingClass::Interior =>
                    matches!(value, CellPreclassValue::Inside | CellPreclassValue::Maybe),
                RingClass::Exterior =>
                    matches!(value, CellPreclassValue::Outside | CellPreclassValue::Maybe),
                RingClass::Boundary => value == CellPreclassValue::Maybe,
            });
        }
    }

    #[test]
    fn cell_grid_hole_probe_matches_exact_classifier() {
        let shape = Shape::Polygon(square_with_hole());
        let tester = PointBatchTester::new(&shape);
        let grid = tester.cell_preclass_for(10_000).unwrap();
        let point = grid.cell_center(20 * CELL_GRID_SIDE + 20);

        assert_cell_matches_exact(&tester, grid, point);
    }

    fn assert_cell_matches_exact(tester: &PointBatchTester, grid: &CellPreclass, point: Point) {
        let exact_contains = tester.contains_point(point);
        let exact_covers = tester.covers_point(point);
        let certificate = tester.cell_classify(grid, point);
        let contains = certificate.map_or(exact_contains, |certificate| {
            certificate == CellCertificate::Interior
        });
        let covers = certificate.map_or(exact_covers, |certificate| {
            certificate == CellCertificate::Interior
        });
        assert_eq!(contains, exact_contains, "contains mismatch at {point:?}");
        assert_eq!(covers, exact_covers, "covers mismatch at {point:?}");
        if tester.classify_area_point(point) == Some(RingClass::Boundary) {
            assert_eq!(grid.lookup(point), CellPreclassValue::Maybe, "{point:?}");
        }
    }

    #[test]
    fn cell_grid_marks_both_sides_of_stored_axis_lines() {
        let xs = CellPreclass::axis_boundaries(0.0, 0.6);
        let ys = CellPreclass::axis_boundaries(0.0, 0.6);
        let xline = xs[31];
        let yline = ys[37];
        let shapes = [
            Shape::Polygon(Polygon {
                shell: closed_ring(&[
                    (0.0, 0.0),
                    (0.6, 0.0),
                    (0.6, 0.6),
                    (xline, 0.6),
                    (xline, 0.2),
                    (0.2, 0.2),
                    (0.2, 0.6),
                    (0.0, 0.6),
                    (0.0, 0.0),
                ]),
                holes: Arc::from([]),
            }),
            Shape::Polygon(Polygon {
                shell: closed_ring(&[
                    (0.0, 0.0),
                    (0.6, 0.0),
                    (0.6, 0.6),
                    (0.4, 0.6),
                    (0.4, yline),
                    (0.2, yline),
                    (0.2, 0.6),
                    (0.0, 0.6),
                    (0.0, 0.0),
                ]),
                holes: Arc::from([]),
            }),
        ];
        assert_eq!(CellPreclass::axis_cell(&xs, xline), 31);
        for (index, shape) in shapes.into_iter().enumerate() {
            let tester = PointBatchTester::new(&shape);
            let grid = tester.cell_preclass_for(10_000).unwrap();
            if index == 0 {
                for row in 0..CELL_GRID_SIDE {
                    if grid.y_boundary(row) < 0.6 && grid.y_boundary(row + 1) > 0.2 {
                        assert_eq!(
                            grid.cells[row * CELL_GRID_SIDE + 30],
                            CellPreclassValue::Maybe
                        );
                        assert_eq!(
                            grid.cells[row * CELL_GRID_SIDE + 31],
                            CellPreclassValue::Maybe
                        );
                    }
                }
                for point in [
                    Point::new_unchecked_xy(xline, 0.4),
                    grid.cell_center(30),
                    grid.cell_center(31),
                    Point::new_unchecked_xy(xline.next_down(), 0.4),
                    Point::new_unchecked_xy(xline.next_up(), 0.4),
                ] {
                    assert_cell_matches_exact(&tester, grid, point);
                }
            } else {
                for col in 0..CELL_GRID_SIDE {
                    if grid.x_boundary(col) < 0.4 && grid.x_boundary(col + 1) > 0.2 {
                        assert_eq!(
                            grid.cells[36 * CELL_GRID_SIDE + col],
                            CellPreclassValue::Maybe
                        );
                        assert_eq!(
                            grid.cells[37 * CELL_GRID_SIDE + col],
                            CellPreclassValue::Maybe
                        );
                    }
                }
                for point in [
                    Point::new_unchecked_xy(0.3, yline),
                    grid.cell_center(36 * CELL_GRID_SIDE + 10),
                    grid.cell_center(37 * CELL_GRID_SIDE + 10),
                    Point::new_unchecked_xy(0.3, yline.next_down()),
                    Point::new_unchecked_xy(0.3, yline.next_up()),
                ] {
                    assert_cell_matches_exact(&tester, grid, point);
                }
            }
        }
    }

    fn sample01(state: &mut u64) -> f64 {
        *state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((*state >> 11) as f64) * (1.0 / ((1_u64 << 53) as f64))
    }

    #[test]
    fn cell_certificates_match_exact_for_seeded_collinear_corpus() {
        let mut state = 0x6A09_E667_F3BC_C909_u64;
        for i in 0..16 {
            let minx = sample01(&mut state) * 3.0 + 0.1;
            let miny = sample01(&mut state) * 3.0 + 0.2;
            let maxx = minx + sample01(&mut state) * 4.0 + 1.0;
            let maxy = miny + sample01(&mut state) * 4.0 + 1.0;
            let xs = CellPreclass::axis_boundaries(minx, maxx);
            let ys = CellPreclass::axis_boundaries(miny, maxy);
            let xi = 1 + (i * 17 % 63);
            let yi = 1 + (i * 29 % 63);
            let notch_x = minx + (maxx - minx) * 0.3;
            let notch_y = miny + (maxy - miny) * 0.4;
            let shape = if i % 2 == 0 {
                Shape::Polygon(Polygon {
                    shell: closed_ring(&[
                        (minx, miny),
                        (maxx, miny),
                        (maxx, maxy),
                        (xs[xi], maxy),
                        (xs[xi], miny + (maxy - miny) * 0.4),
                        (notch_x, notch_y),
                        (notch_x, maxy),
                        (minx, maxy),
                        (minx, miny),
                    ]),
                    holes: Arc::from([]),
                })
            } else {
                Shape::Polygon(Polygon {
                    shell: closed_ring(&[
                        (minx, miny),
                        (maxx, miny),
                        (maxx, maxy),
                        (minx + (maxx - minx) * 0.7, maxy),
                        (minx + (maxx - minx) * 0.7, ys[yi]),
                        (notch_x, ys[yi]),
                        (notch_x, maxy),
                        (minx, maxy),
                        (minx, miny),
                    ]),
                    holes: Arc::from([]),
                })
            };
            let mut endpoint_grid = CellPreclass {
                bounds: Bounds::new_unchecked(minx, miny, maxx, maxy),
                x_boundaries: xs,
                y_boundaries: ys,
                cells: Box::new([CellPreclassValue::Outside; CELL_GRID_LEN]),
            };
            let endpoint_x = xs[xi] + (xs[xi + 1] - xs[xi]) * 0.25;
            let endpoint_y = ys[yi] + (ys[yi + 1] - ys[yi]) * 0.25;
            endpoint_grid.mark_endpoint(endpoint_x, endpoint_y);
            assert_eq!(
                endpoint_grid.cells[yi * CELL_GRID_SIDE + xi],
                CellPreclassValue::Maybe,
                "endpoint cell at shape {i}, point ({endpoint_x}, {endpoint_y})"
            );
            assert_eq!(
                endpoint_grid.cells[(yi - 1) * CELL_GRID_SIDE + xi - 1],
                CellPreclassValue::Outside,
                "endpoint predecessor at shape {i}, point ({endpoint_x}, {endpoint_y})"
            );
            let tester = PointBatchTester::new(&shape);
            let grid = tester.cell_preclass_for(10_000).unwrap();
            for point_index in 0..4096 {
                let point = if point_index % 8 == 0 {
                    if i % 2 == 0 {
                        Point::new_unchecked_xy(xs[xi], miny + sample01(&mut state) * (maxy - miny))
                    } else {
                        Point::new_unchecked_xy(minx + sample01(&mut state) * (maxx - minx), ys[yi])
                    }
                } else if point_index % 8 == 1 {
                    if i % 2 == 0 {
                        Point::new_unchecked_xy(xs[xi], notch_y)
                    } else {
                        Point::new_unchecked_xy(notch_x, ys[yi])
                    }
                } else if point_index % 8 == 2 {
                    if i % 2 == 0 {
                        Point::new_unchecked_xy(xs[xi].next_up(), notch_y)
                    } else {
                        Point::new_unchecked_xy(notch_x, ys[yi].next_up())
                    }
                } else {
                    Point::new_unchecked_xy(
                        minx + (sample01(&mut state) * 1.2 - 0.1) * (maxx - minx),
                        miny + (sample01(&mut state) * 1.2 - 0.1) * (maxy - miny),
                    )
                };
                assert_cell_matches_exact(&tester, grid, point);
            }
        }
    }

    #[test]
    fn cell_grid_selectivity_observation() {
        let shape = regular_polygon(512);
        let tester = PointBatchTester::new(&shape);
        let grid = tester.cell_preclass_for(10_000).unwrap();
        let mut maybe = 0;
        for row in 0..CELL_GRID_SIDE {
            for col in 0..CELL_GRID_SIDE {
                maybe +=
                    usize::from(grid.cells[row * CELL_GRID_SIDE + col] == CellPreclassValue::Maybe);
            }
        }
        println!("512-edge selectivity: Maybe={maybe}/{CELL_GRID_LEN}");
    }

    #[test]
    fn degenerate_and_zero_length_edges_fail_open_without_grid() {
        let shape = Shape::Polygon(Polygon {
            shell: closed_ring(&[(0.0, 0.0), (0.0, 0.0), (0.0, 0.0)]),
            holes: Arc::from([]),
        });
        let tester = PointBatchTester::new(&shape);
        assert!(tester.cell_preclass_for(10_000).is_none());
        for point in [
            Point::new_unchecked_xy(0.0, 0.0),
            Point::new_unchecked_xy(1.0, 1.0),
            Point::new_unchecked_xy(-1.0, 0.0),
        ] {
            assert_eq!(tester.contains_point(point), shape.contains_point(point));
            assert_eq!(tester.covers_point(point), shape.covers_point(point));
        }
    }

    #[test]
    fn concurrent_grid_initialization_is_accounted_once() {
        let tester = Arc::new(PointBatchTester::new(&regular_polygon(64)));
        let before = tester.heap_bytes();
        let threads: [_; 8] = std::array::from_fn(|_| {
            let tester = Arc::clone(&tester);
            std::thread::spawn(move || tester.cell_preclass_for(10_000).is_some())
        });
        assert!(threads.into_iter().all(|thread| thread.join().unwrap()));
        assert!(tester.heap_bytes() > before);
        assert!(tester.cell_preclass_for(10_000).is_some());
    }
}
