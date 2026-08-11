//! Shape-wide coordinate access — the structural `SoA` view behind
//! `geom.coords`.
//!
//! [`Coordinates`](crate::geometry::Coordinates) stays the small by-value
//! nested rings, collections), flattens coordinates in canonical depth-first
//! order, and borrows the underlying [`CoordSeq`] columns directly when the
//! view is one contiguous run so per-axis access stays zero-copy.
//!
//! Later build-order increments grow this with run iteration for columnar Arrow
//! export, alternate owners (`array.coords` over packed storage), and a tuple
//! layout selector — each added alongside the consumer that needs it.

use std::ops::ControlFlow;
use std::sync::Arc;

use crate::GeometryArrayStorage;
use crate::array::MissingMask;
use crate::geometry::{
    CoordSeq, CoordWindow, CoordinateAxes, MOrdinate, Point, Shape, ShapeData, ZOrdinate,
};

/// One ordinate in a coordinate layout.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoordinateAxis {
    X,
    Y,
    Z,
    M,
}

/// Stable provenance for a flattened coordinate — enough to reconstruct
/// `coords(return_index=True)` and richer indexes without changing order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CoordinatePath {
    /// Geometry row for array owners; `None` for a scalar geometry owner.
    pub geometry: Option<usize>,
    /// Top-level part index inside the owning geometry.
    pub part: usize,
    /// Ring index for polygonal coordinates: `0` exterior, `1..` interiors.
    pub ring: Option<usize>,
    /// Coordinate offset inside that part/ring run.
    pub coord: usize,
}

/// A coordinate plus its path metadata.
#[derive(Clone, Copy, Debug)]
pub struct CoordinatePoint {
    pub point: Point,
    pub path: CoordinatePath,
}

/// A per-axis column resolved from a view: borrowed when the view is one
/// contiguous run, materialized otherwise.
pub enum CoordinateColumnRef<'a> {
    /// A borrowed contiguous `f64` column.
    Borrowed(&'a [f64]),
    /// Dense but gathered across runs (X/Y over a multipart view).
    Dense(Vec<f64>),
    /// Z/M over a view where some coordinates lack the ordinate; `None` where
    /// absent.
    Nullable(Vec<Option<f64>>),
    /// No coordinate in the view carries this ordinate.
    Missing,
}

/// One flattened coordinate run. A run is either a contiguous coordinate
/// sequence or one scalar point; never both and never neither.
#[derive(Clone, Copy)]
enum CoordinateRun<'a> {
    Seq(&'a CoordSeq),
    Point(Point),
}

/// Owner handle for a coordinate view. Both variants are immutable and cheap to
/// clone; the `Arc` lets the view outlive the borrow that produced it without
/// copying coordinates. Packed [`GeometryArrayStorage`] variants sit behind
/// the `Array` owner.
#[derive(Clone, Debug)]
enum CoordinateOwner {
    Shape(Arc<ShapeData>),
    Array {
        storage: Arc<GeometryArrayStorage>,
        missing: Option<MissingMask>,
    },
}

/// The geometry source behind a view, for nested (topology-shaped) coordinate
/// rendering that the flat columns intentionally do not preserve.
pub enum CoordinateSource<'a> {
    Shape(&'a Shape),
    Array(&'a GeometryArrayStorage, Option<&'a [bool]>),
}

/// Shape-wide coordinate access. Flattening order matches
/// [`Shape::points`](crate::geometry::Shape::points): point, then each part /
/// ring in declaration order, exterior ring before interiors. An array owner
/// flattens its geometries in row order, each coordinate tagged with its row.
#[derive(Clone, Debug)]
pub struct CoordinateView {
    owner: CoordinateOwner,
}

impl CoordinateView {
    pub const fn from_shape(shape: Arc<ShapeData>) -> Self {
        Self {
            owner: CoordinateOwner::Shape(shape),
        }
    }

    pub const fn from_array(storage: Arc<GeometryArrayStorage>) -> Self {
        Self::from_array_masked(storage, None)
    }

    pub(crate) const fn from_array_masked(
        storage: Arc<GeometryArrayStorage>,
        missing: Option<MissingMask>,
    ) -> Self {
        Self {
            owner: CoordinateOwner::Array { storage, missing },
        }
    }

    /// The geometry source, for topology-shaped (nested) rendering.
    pub fn source(&self) -> CoordinateSource<'_> {
        match &self.owner {
            CoordinateOwner::Shape(shape) => CoordinateSource::Shape(shape),
            CoordinateOwner::Array { storage, missing } => {
                CoordinateSource::Array(storage, missing.as_deref())
            },
        }
    }

    /// Drive `visit` over every run / scalar point of the owner, dispatching
    /// scalar shapes vs array rows. The traversal primitive behind every
    /// method. Prefer [`try_for_each`] when the visitor may stop early.
    fn for_each<F>(&self, visit: &mut F)
    where
        F: for<'x> FnMut(Option<usize>, usize, Option<usize>, CoordinateRun<'x>),
    {
        let _: ControlFlow<()> = self.try_for_each(&mut |geometry, part, ring, run| {
            visit(geometry, part, ring, run);
            ControlFlow::Continue(())
        });
    }

    /// Breakable traversal: `ControlFlow::Break` stops at the current run.
    fn try_for_each<B, F>(&self, visit: &mut F) -> ControlFlow<B>
    where
        F: for<'x> FnMut(Option<usize>, usize, Option<usize>, CoordinateRun<'x>) -> ControlFlow<B>,
    {
        match &self.owner {
            CoordinateOwner::Shape(shape) => try_walk(shape, None, visit),
            CoordinateOwner::Array { storage, missing } => match storage.as_ref() {
                GeometryArrayStorage::Mixed(shapes) => {
                    for (row, shape) in shapes.iter().enumerate() {
                        if missing.as_ref().is_some_and(|mask| mask[row]) {
                            continue;
                        }
                        try_walk(shape, Some(row), visit)?;
                    }
                    ControlFlow::Continue(())
                },
                // Packed points: each row is one scalar point geometry, so it
                // contributes one coordinate tagged with its own row index.
                GeometryArrayStorage::Points { coords, row_map } => {
                    let map = row_map.as_deref();
                    for row in 0..crate::array::point_logical_len(coords, map) {
                        if missing.as_ref().is_some_and(|mask| mask[row]) {
                            continue;
                        }
                        visit(
                            Some(row),
                            0,
                            None,
                            CoordinateRun::Point(
                                coords.point_at(crate::array::physical_row(map, row)),
                            ),
                        )?;
                    }
                    ControlFlow::Continue(())
                },
                // Packed lines: each row contributes its zero-copy column
                // window as one run, exactly like a scalar LineString.
                GeometryArrayStorage::Lines {
                    coords,
                    offsets,
                    row_map,
                } => {
                    let map = row_map.as_deref();
                    let rows = crate::array::line_logical_len(offsets.as_slice(), map);
                    for row in 0..rows {
                        if missing.as_ref().is_some_and(|mask| mask[row]) {
                            continue;
                        }
                        let window = map.csr_window(offsets.as_slice(), row);
                        let view = coords.view(CoordWindow::trusted(window, coords.len()));
                        visit(Some(row), 0, None, CoordinateRun::Seq(&view))?;
                    }
                    ControlFlow::Continue(())
                },
                // Packed polygons: walk the two CSR levels directly. Building
                // a temporary Polygon view per row would allocate its hole Arc
                // and then make `walk` traverse the same offsets again.
                GeometryArrayStorage::Polygons {
                    coords,
                    ring_offsets,
                    polygon_offsets,
                    row_map,
                } => {
                    let map = row_map.as_deref();
                    let ring_offsets = ring_offsets.as_slice();
                    let rows = crate::array::polygon_logical_len(polygon_offsets.as_slice(), map);
                    for row in 0..rows {
                        if missing.as_ref().is_some_and(|mask| mask[row]) {
                            continue;
                        }
                        for (ring, physical_ring) in
                            map.csr_window(polygon_offsets.as_slice(), row).enumerate()
                        {
                            let window = ring_offsets[physical_ring] as usize
                                ..ring_offsets[physical_ring + 1] as usize;
                            let view = coords.view(CoordWindow::trusted(window, coords.len()));
                            visit(Some(row), 0, Some(ring), CoordinateRun::Seq(&view))?;
                        }
                    }
                    ControlFlow::Continue(())
                },
            },
        }
    }

    /// Number of flattened coordinates. Structural — never materializes.
    pub fn len(&self) -> usize {
        match &self.owner {
            CoordinateOwner::Shape(_) => {
                let mut total = 0;
                self.for_each(&mut |_, _, _, run| match run {
                    CoordinateRun::Seq(seq) => total += seq.len(),
                    CoordinateRun::Point(_) => total += 1,
                });
                total
            },
            CoordinateOwner::Array { storage, missing } => {
                if missing.is_some() {
                    let mut total = 0;
                    self.for_each(&mut |_, _, _, run| match run {
                        CoordinateRun::Seq(seq) => total += seq.len(),
                        CoordinateRun::Point(_) => total += 1,
                    });
                    return total;
                }
                match storage.as_ref() {
                    GeometryArrayStorage::Points { coords, row_map } => {
                        crate::array::point_logical_len(coords, row_map.as_deref())
                    },
                    GeometryArrayStorage::Lines {
                        offsets, row_map, ..
                    } => {
                        crate::array::packed_lines_coord_len(offsets.as_slice(), row_map.as_deref())
                    },
                    GeometryArrayStorage::Polygons {
                        ring_offsets,
                        polygon_offsets,
                        row_map,
                        ..
                    } => crate::array::packed_polygons_coord_len(
                        ring_offsets.as_slice(),
                        polygon_offsets.as_slice(),
                        row_map.as_deref(),
                    ),
                    GeometryArrayStorage::Mixed(shapes) => {
                        shapes.iter().map(Shape::coord_count).sum()
                    },
                }
            },
        }
    }

    /// Exact flattened size from storage metadata. Unlike `len`, this never
    /// constructs coordinate runs or temporary polygon views; masked packed
    /// arrays need only a cheap offsets/count pass.
    fn flattened_capacity(&self) -> usize {
        match &self.owner {
            CoordinateOwner::Shape(shape) => shape.shape().coord_count(),
            CoordinateOwner::Array {
                storage,
                missing: Some(missing),
            } => match storage.as_ref() {
                GeometryArrayStorage::Points { coords, row_map } => {
                    let rows = crate::array::point_logical_len(coords, row_map.as_deref());
                    (0..rows).filter(|&row| !missing[row]).count()
                },
                GeometryArrayStorage::Lines {
                    offsets, row_map, ..
                } => {
                    let map = row_map.as_deref();
                    let rows = crate::array::line_logical_len(offsets.as_slice(), map);
                    (0..rows)
                        .filter(|&row| !missing[row])
                        .map(|row| map.csr_window(offsets.as_slice(), row).len())
                        .sum()
                },
                GeometryArrayStorage::Polygons {
                    ring_offsets,
                    polygon_offsets,
                    row_map,
                    ..
                } => {
                    let map = row_map.as_deref();
                    let rings = ring_offsets.as_slice();
                    let rows = crate::array::polygon_logical_len(polygon_offsets.as_slice(), map);
                    (0..rows)
                        .filter(|&row| !missing[row])
                        .map(|row| {
                            let row_rings = map.csr_window(polygon_offsets.as_slice(), row);
                            if row_rings.is_empty() {
                                0
                            } else {
                                (rings[row_rings.end] - rings[row_rings.start]) as usize
                            }
                        })
                        .sum()
                },
                GeometryArrayStorage::Mixed(shapes) => shapes
                    .iter()
                    .zip(missing.iter())
                    .filter(|(_, missing)| !**missing)
                    .map(|(shape, _)| shape.coord_count())
                    .sum(),
            },
            CoordinateOwner::Array {
                storage,
                missing: None,
            } => match storage.as_ref() {
                GeometryArrayStorage::Points { coords, row_map } => {
                    crate::array::point_logical_len(coords, row_map.as_deref())
                },
                GeometryArrayStorage::Lines {
                    offsets, row_map, ..
                } => crate::array::packed_lines_coord_len(offsets.as_slice(), row_map.as_deref()),
                GeometryArrayStorage::Polygons {
                    ring_offsets,
                    polygon_offsets,
                    row_map,
                    ..
                } => crate::array::packed_polygons_coord_len(
                    ring_offsets.as_slice(),
                    polygon_offsets.as_slice(),
                    row_map.as_deref(),
                ),
                GeometryArrayStorage::Mixed(shapes) => shapes.iter().map(Shape::coord_count).sum(),
            },
        }
    }

    /// Logical coordinate payload in bytes for this flattened view.
    pub fn nbytes(&self) -> usize {
        match &self.owner {
            CoordinateOwner::Shape(shape) => shape.shape().coordinate_bytes(),
            CoordinateOwner::Array { storage, missing } => {
                if missing.is_some() {
                    // Present coordinates only, at the STORED ordinate width
                    // (an XY view is 16 B/coordinate, not `size_of::<Point>()`
                    // — the interchange struct's 40 B would overreport).
                    let mut bytes = 0_usize;
                    self.for_each(&mut |_, _, _, run| {
                        bytes += match run {
                            CoordinateRun::Seq(seq) => seq.axes().byte_width(seq.len()),
                            CoordinateRun::Point(point) => {
                                CoordinateAxes::from_point(point).byte_width(1)
                            },
                        };
                    });
                    return bytes;
                }
                storage.logical_coordinate_bytes()
            },
        }
    }

    /// Logical Rust-side heap retained by the view's owner.
    pub fn logical_heap_bytes(&self) -> usize {
        match &self.owner {
            CoordinateOwner::Shape(shape) => crate::HeapSize::heap_bytes(shape.shape()),
            CoordinateOwner::Array { storage, .. } => storage.logical_heap_bytes(),
        }
    }

    /// Union of every coordinate's axes — Z/M present if present anywhere.
    ///
    /// For a scalar geometry owner this is the shape's declared axes, so a
    /// typed empty (`POINT Z EMPTY`, `POLYGON M EMPTY`, …) reports its carried
    /// empty axes even though the view has zero vertices. Array owners still
    /// fold over present rows (missing rows contribute nothing).
    pub fn axes(&self) -> CoordinateAxes {
        match &self.owner {
            CoordinateOwner::Shape(shape) => shape.shape().axes(),
            CoordinateOwner::Array { .. } => {
                let mut axes = CoordinateAxes::XY;
                self.for_each(&mut |_, _, _, run| match run {
                    CoordinateRun::Seq(seq) => axes = axes.union(seq.axes()),
                    CoordinateRun::Point(point) => {
                        axes = axes.union(CoordinateAxes::from_point(point));
                    },
                });
                axes
            },
        }
    }

    /// `Some` only when the whole view is exactly one contiguous [`CoordSeq`]
    /// (`MultiPoint`/`LineString`), enabling zero-copy columns. `None` for
    /// `Point` and any multipart view.
    pub fn single_seq(&self) -> Option<&CoordSeq> {
        match &self.owner {
            CoordinateOwner::Shape(shape) => match shape.shape() {
                Shape::MultiPoint(seq) => Some(seq),
                Shape::LineString(seq) => Some(seq.as_coords()),
                _ => None,
            },
            // A packed point array's coordinates are exactly its shared column,
            // so `.x`/`.y` can borrow it directly (zero-copy on the Rust side).
            CoordinateOwner::Array { storage, missing } => {
                if missing.is_some() {
                    return None;
                }
                match storage.as_ref() {
                    GeometryArrayStorage::Points { coords, row_map } => {
                        row_map.is_identity().then_some(coords)
                    },
                    // Packed line/polygon coordinate columns borrow only when the
                    // logical rows are the whole physical storage. Row-selected
                    // arrays materialize through `for_each`, so public buffers
                    // match logical row order and never expose excluded parent
                    // coordinates.
                    GeometryArrayStorage::Lines {
                        coords, row_map, ..
                    }
                    | GeometryArrayStorage::Polygons {
                        coords, row_map, ..
                    } => row_map.is_identity().then_some(coords),
                    GeometryArrayStorage::Mixed(_) => None,
                }
            },
        }
    }

    /// Resolve one axis to a borrowed or materialized column.
    pub fn column(&self, axis: CoordinateAxis) -> CoordinateColumnRef<'_> {
        if let CoordinateOwner::Array { storage, missing } = &self.owner
            && let GeometryArrayStorage::Points { coords, row_map } = storage.as_ref()
            && missing.is_none()
        {
            if let Some(window) = row_map.as_deref().contiguous_window() {
                return match axis {
                    CoordinateAxis::X => CoordinateColumnRef::Borrowed(&coords.xs()[window]),
                    CoordinateAxis::Y => CoordinateColumnRef::Borrowed(&coords.ys()[window]),
                    CoordinateAxis::Z => coords.zs().map_or(CoordinateColumnRef::Missing, |zs| {
                        CoordinateColumnRef::Borrowed(&zs[window])
                    }),
                    CoordinateAxis::M => coords.ms().map_or(CoordinateColumnRef::Missing, |ms| {
                        CoordinateColumnRef::Borrowed(&ms[window])
                    }),
                };
            }
            if let Some(map) = row_map.explicit_indices() {
                let values = match axis {
                    CoordinateAxis::X => coords.xs(),
                    CoordinateAxis::Y => coords.ys(),
                    CoordinateAxis::Z => {
                        return coords.zs().map_or(CoordinateColumnRef::Missing, |zs| {
                            CoordinateColumnRef::Dense(map.iter().map(|&row| zs[row]).collect())
                        });
                    },
                    CoordinateAxis::M => {
                        return coords.ms().map_or(CoordinateColumnRef::Missing, |ms| {
                            CoordinateColumnRef::Dense(map.iter().map(|&row| ms[row]).collect())
                        });
                    },
                };
                return CoordinateColumnRef::Dense(map.iter().map(|&row| values[row]).collect());
            }
        }
        if let Some(seq) = self.single_seq() {
            return match axis {
                CoordinateAxis::X => CoordinateColumnRef::Borrowed(seq.xs()),
                CoordinateAxis::Y => CoordinateColumnRef::Borrowed(seq.ys()),
                CoordinateAxis::Z => seq
                    .zs()
                    .map_or(CoordinateColumnRef::Missing, CoordinateColumnRef::Borrowed),
                CoordinateAxis::M => seq
                    .ms()
                    .map_or(CoordinateColumnRef::Missing, CoordinateColumnRef::Borrowed),
            };
        }
        match axis {
            CoordinateAxis::X => CoordinateColumnRef::Dense(self.gather_x()),
            CoordinateAxis::Y => CoordinateColumnRef::Dense(self.gather_y()),
            CoordinateAxis::Z => self.gather_optional(Point::z),
            CoordinateAxis::M => self.gather_optional(Point::m),
        }
    }

    /// Stream every flattened coordinate (with path metadata) without
    /// materializing a `Vec<CoordinatePoint>` — the engine behind the list /
    /// numpy / row-index surfaces. Use [`collect_points`](Self::collect_points)
    /// only where random access is genuinely needed (slicing).
    #[expect(
        clippy::impl_trait_in_params,
        reason = "visitor type is intentionally opaque at this one-pass traversal boundary"
    )]
    pub fn for_each_point(&self, mut visit: impl FnMut(CoordinatePoint)) {
        let _: ControlFlow<()> = self.try_for_each_point(&mut |coord| {
            visit(coord);
            ControlFlow::Continue(())
        });
    }

    /// Breakable point stream: `ControlFlow::Break` stops at the current
    /// coordinate. Used by `index` / membership / equality so a first hit or
    /// first mismatch never walks the tail.
    #[expect(
        clippy::impl_trait_in_params,
        reason = "visitor type is intentionally opaque at this one-pass traversal boundary"
    )]
    pub fn try_for_each_point<B>(
        &self,
        visit: &mut impl FnMut(CoordinatePoint) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        self.try_for_each(&mut |geometry, part, ring, run| match run {
            CoordinateRun::Seq(seq) => {
                // Length-exact column windows: loop bound IS each slice's len,
                // so per-vertex access carries no bounds checks (elision).
                let xs = seq.xs();
                let ys = seq.ys();
                let width = xs.len();
                let ys = &ys[..width];
                let zs = seq.zs().map(|column| &column[..width]);
                let ms = seq.ms().map(|column| &column[..width]);
                for coord in 0..width {
                    visit(CoordinatePoint {
                        point: Point::new_unchecked_axes(
                            xs[coord],
                            ys[coord],
                            ZOrdinate(zs.map(|column| column[coord])),
                            MOrdinate(ms.map(|column| column[coord])),
                        ),
                        path: CoordinatePath {
                            geometry,
                            part,
                            ring,
                            coord,
                        },
                    })?;
                }
                ControlFlow::Continue(())
            },
            CoordinateRun::Point(point) => visit(CoordinatePoint {
                point,
                path: CoordinatePath {
                    geometry,
                    part,
                    ring,
                    coord: 0,
                },
            }),
        })
    }

    /// Flattened coordinates with path metadata.
    pub fn collect_points(&self) -> Vec<CoordinatePoint> {
        let mut points = Vec::with_capacity(self.flattened_capacity());
        self.for_each_point(|coord| points.push(coord));
        points
    }

    /// Per-coordinate geometry-row index (all `0` for a scalar geometry; grows
    /// to real rows when `array.coords` lands). Backs
    /// `coords(return_index=True)`.
    pub fn row_index(&self) -> Vec<usize> {
        self.row_index_cast(|row| row)
    }

    /// Per-coordinate geometry-row index as NumPy's `int64` lane, built at
    /// coordinate-run altitude (CSR window fills / one fill per run) rather
    /// than per-vertex push. Mirrors [`row_index`] for the `Coordinates.index`
    /// getter and `get_coordinates(..., return_index=True)`.
    pub fn row_index_i64(&self) -> Vec<i64> {
        self.row_index_cast(|row| row as i64)
    }

    /// Shared run-wise row-index emitter. Identity packed storage fills from
    /// CSR offsets (or the point column length); every other owner walks
    /// coordinate runs once and fills each run's span with its logical row id.
    fn row_index_cast<T: Copy>(&self, cast: impl Fn(usize) -> T) -> Vec<T> {
        let capacity = self.flattened_capacity();
        let zero = cast(0);

        if let CoordinateOwner::Array {
            storage,
            missing: None,
        } = &self.owner
        {
            match storage.as_ref() {
                GeometryArrayStorage::Points { coords, row_map } if row_map.is_identity() => {
                    return (0..coords.len()).map(cast).collect();
                },
                GeometryArrayStorage::Lines {
                    offsets, row_map, ..
                } if row_map.is_identity() => {
                    let offsets = offsets.as_slice();
                    let mut indexes = vec![zero; capacity];
                    let n_rows = offsets.len().saturating_sub(1);
                    for row in 0..n_rows {
                        let start = offsets[row] as usize;
                        let end = offsets[row + 1] as usize;
                        indexes[start..end].fill(cast(row));
                    }
                    return indexes;
                },
                GeometryArrayStorage::Polygons {
                    ring_offsets,
                    polygon_offsets,
                    row_map,
                    ..
                } if row_map.is_identity() => {
                    let rings = ring_offsets.as_slice();
                    let polys = polygon_offsets.as_slice();
                    let mut indexes = vec![zero; capacity];
                    let n_rows = polys.len().saturating_sub(1);
                    for row in 0..n_rows {
                        let ring_start = polys[row] as usize;
                        let ring_end = polys[row + 1] as usize;
                        if ring_start == ring_end {
                            continue;
                        }
                        let start = rings[ring_start] as usize;
                        let end = rings[ring_end] as usize;
                        indexes[start..end].fill(cast(row));
                    }
                    return indexes;
                },
                _ => {},
            }
        }

        // Scalar geometry: every coordinate belongs to the single owner (id 0).
        if matches!(&self.owner, CoordinateOwner::Shape(_)) {
            return vec![zero; capacity];
        }

        // Mask / gather / window / mixed: one fill per coordinate run, logical
        // row ids, missing rows skipped by `for_each`.
        let mut indexes = vec![zero; capacity];
        let mut cursor = 0_usize;
        self.for_each(&mut |geometry, _, _, run| {
            let len = match run {
                CoordinateRun::Seq(seq) => seq.len(),
                CoordinateRun::Point(_) => 1,
            };
            let id = cast(geometry.unwrap_or(0));
            indexes[cursor..cursor + len].fill(id);
            cursor += len;
        });
        debug_assert_eq!(cursor, capacity);
        indexes
    }

    /// The coordinate at `index` (flattened), or `None` if out of range.
    ///
    /// Packed identity columns and single-sequence shapes resolve in O(1) (or
    /// O(log runs) via CSR binary search). General owners walk runs with early
    /// break so `coords[0]` never scans past the first hit.
    pub fn point_at(&self, index: usize) -> Option<CoordinatePoint> {
        if let Some(point) = self.point_at_fast(index) {
            return Some(point);
        }
        let mut remaining = index;
        let result = self.try_for_each(&mut |geometry, part, ring, run| match run {
            CoordinateRun::Seq(seq) => {
                let len = seq.len();
                if remaining < len {
                    ControlFlow::Break(CoordinatePoint {
                        point: seq.point_at(remaining),
                        path: CoordinatePath {
                            geometry,
                            part,
                            ring,
                            coord: remaining,
                        },
                    })
                } else {
                    remaining -= len;
                    ControlFlow::Continue(())
                }
            },
            CoordinateRun::Point(point) => {
                if remaining == 0 {
                    ControlFlow::Break(CoordinatePoint {
                        point,
                        path: CoordinatePath {
                            geometry,
                            part,
                            ring,
                            coord: 0,
                        },
                    })
                } else {
                    remaining -= 1;
                    ControlFlow::Continue(())
                }
            },
        });
        match result {
            ControlFlow::Break(point) => Some(point),
            ControlFlow::Continue(()) => None,
        }
    }

    /// Storage-shaped random access for packed identity / window / gather
    /// columns and single-run shapes. O(1) for points; O(log rows) for
    /// contiguous CSR; gather of lines/polys falls through to the run walker.
    fn point_at_fast(&self, index: usize) -> Option<CoordinatePoint> {
        match &self.owner {
            CoordinateOwner::Shape(shape) => match shape.shape() {
                Shape::Point(point) if index == 0 => Some(CoordinatePoint {
                    point: *point,
                    path: CoordinatePath {
                        geometry: None,
                        part: 0,
                        ring: None,
                        coord: 0,
                    },
                }),
                Shape::LineString(seq) if index < seq.len() => Some(CoordinatePoint {
                    point: seq.point_at(index),
                    path: CoordinatePath {
                        geometry: None,
                        part: 0,
                        ring: None,
                        coord: index,
                    },
                }),
                Shape::MultiPoint(seq) if index < seq.len() => Some(CoordinatePoint {
                    point: seq.point_at(index),
                    path: CoordinatePath {
                        geometry: None,
                        part: 0,
                        ring: None,
                        coord: index,
                    },
                }),
                _ => None,
            },
            CoordinateOwner::Array {
                storage,
                missing: None,
            } => match storage.as_ref() {
                GeometryArrayStorage::Points { coords, row_map } => {
                    let map = row_map.as_deref();
                    let len = crate::array::point_logical_len(coords, map);
                    if index >= len {
                        return None;
                    }
                    let physical = map.physical(index);
                    Some(CoordinatePoint {
                        point: coords.point_at(physical),
                        path: CoordinatePath {
                            geometry: Some(index),
                            part: 0,
                            ring: None,
                            coord: 0,
                        },
                    })
                },
                GeometryArrayStorage::Lines {
                    coords,
                    offsets,
                    row_map,
                } => {
                    let map = row_map.as_deref();
                    let offsets = offsets.as_slice();
                    match map {
                        crate::array::RowSelectionRef::Identity
                        | crate::array::RowSelectionRef::Window { .. } => {
                            packed_line_point_at(coords, offsets, map, index)
                        },
                        crate::array::RowSelectionRef::Gather(_) => None,
                    }
                },
                GeometryArrayStorage::Polygons {
                    coords,
                    ring_offsets,
                    polygon_offsets,
                    row_map,
                } => {
                    let map = row_map.as_deref();
                    match map {
                        crate::array::RowSelectionRef::Identity
                        | crate::array::RowSelectionRef::Window { .. } => packed_polygon_point_at(
                            coords,
                            ring_offsets.as_slice(),
                            polygon_offsets.as_slice(),
                            map,
                            index,
                        ),
                        crate::array::RowSelectionRef::Gather(_) => None,
                    }
                },
                // Mixed / masked / gather-of-ragged: flat index is not a
                // storage-shaped O(1) key — fall through to the run walker.
                GeometryArrayStorage::Mixed(_) => None,
            },
            CoordinateOwner::Array {
                missing: Some(_), ..
            } => None,
        }
    }

    /// Visible-value equality of two flattened views. Uses owner-identity and
    /// packed columnar bulk paths for full equal scans, and a dual sequential
    /// stream for the general / early-mismatch case. Never indexes the right
    /// side with per-coordinate CSR `point_at` (that is O(n log rows)).
    pub fn equal_visible(
        &self,
        other: &Self,
        self_layout: Option<CoordinateAxes>,
        other_layout: Option<CoordinateAxes>,
    ) -> bool {
        if self.len() != other.len() {
            return false;
        }
        // Same owner Arc (+ missing mask): identical flattened content.
        if owners_identical(&self.owner, &other.owner) && self_layout == other_layout {
            return true;
        }
        // Same packed structure, no layout override: compare coordinate
        // columns run-wise without materializing Points.
        if self_layout.is_none()
            && other_layout.is_none()
            && let Some(equal) = try_columnar_storage_equal(&self.owner, &other.owner)
        {
            return equal;
        }
        // Dual sequential stream: walk left, materialize right once into a
        // visible-value buffer. Full equal is O(n); first mismatch early-exits
        // the left walk (right is already buffered — still O(n) prep, never
        // O(n log rows)).
        let mut right_visible = Vec::with_capacity(other.len());
        other.for_each_point(|coord| {
            right_visible.push(visible_coordinate_bits(coord.point, other_layout));
        });
        let mut idx = 0_usize;
        let equal = self.try_for_each_point(&mut |coord| {
            let left = visible_coordinate_bits(coord.point, self_layout);
            if right_visible.get(idx).copied() != Some(left) {
                return ControlFlow::Break(false);
            }
            idx += 1;
            ControlFlow::Continue(())
        });
        !matches!(equal, ControlFlow::Break(false))
    }

    /// Whether random `point_at(i)` for every `i` is linear overall via the
    /// storage-shaped path (so a cursor may call it once per step). Gather of
    /// ragged lines/polys, mixed multiparts, and masked arrays need a single
    /// materialization instead — repeated walk-from-start is quadratic.
    ///
    /// Packed line/polygon identity/window lookups are O(log rows) each via
    /// CSR binary search — fine for single `coords[i]` / first-hit `index`,
    /// but **not** for full-scan equality (use [`equal_visible`] / run walks).
    pub fn has_o1_random_access(&self) -> bool {
        match &self.owner {
            CoordinateOwner::Shape(shape) => matches!(
                shape.shape(),
                Shape::Point(_) | Shape::LineString(_) | Shape::MultiPoint(_)
            ),
            CoordinateOwner::Array {
                storage,
                missing: None,
            } => match storage.as_ref() {
                GeometryArrayStorage::Points { .. } => true,
                GeometryArrayStorage::Lines { row_map, .. }
                | GeometryArrayStorage::Polygons { row_map, .. } => {
                    !matches!(row_map.as_deref(), crate::array::RowSelectionRef::Gather(_))
                },
                GeometryArrayStorage::Mixed(_) => false,
            },
            CoordinateOwner::Array {
                missing: Some(_), ..
            } => false,
        }
    }

    fn gather_x(&self) -> Vec<f64> {
        let mut out = Vec::with_capacity(self.flattened_capacity());
        self.for_each(&mut |_, _, _, run| match run {
            CoordinateRun::Seq(seq) => out.extend_from_slice(seq.xs()),
            CoordinateRun::Point(point) => out.push(point.x),
        });
        out
    }
}

fn owners_identical(left: &CoordinateOwner, right: &CoordinateOwner) -> bool {
    match (left, right) {
        (CoordinateOwner::Shape(a), CoordinateOwner::Shape(b)) => Arc::ptr_eq(a, b),
        (
            CoordinateOwner::Array {
                storage: sa,
                missing: ma,
            },
            CoordinateOwner::Array {
                storage: sb,
                missing: mb,
            },
        ) => Arc::ptr_eq(sa, sb) && ma == mb,
        _ => false,
    }
}

/// Bit-pattern column equality (NaN-stable, matching `Point`/`PartialEq`).
fn f64_columns_bit_eq(left: &[f64], right: &[f64]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right.iter())
        .all(|(a, b)| a.to_bits() == b.to_bits())
}

fn coordseq_columns_bit_eq(left: &CoordSeq, right: &CoordSeq) -> bool {
    left.axes() == right.axes()
        && f64_columns_bit_eq(left.xs(), right.xs())
        && f64_columns_bit_eq(left.ys(), right.ys())
        && match (left.zs(), right.zs()) {
            (None, None) => true,
            (Some(a), Some(b)) => f64_columns_bit_eq(a, b),
            _ => false,
        }
        && match (left.ms(), right.ms()) {
            (None, None) => true,
            (Some(a), Some(b)) => f64_columns_bit_eq(a, b),
            _ => false,
        }
}

fn row_selection_eq(left: &crate::array::RowSelection, right: &crate::array::RowSelection) -> bool {
    match (left.as_deref(), right.as_deref()) {
        (crate::array::RowSelectionRef::Identity, crate::array::RowSelectionRef::Identity) => true,
        (
            crate::array::RowSelectionRef::Window { start: sa, len: la },
            crate::array::RowSelectionRef::Window { start: sb, len: lb },
        ) => sa == sb && la == lb,
        (crate::array::RowSelectionRef::Gather(a), crate::array::RowSelectionRef::Gather(b)) => {
            a == b
        },
        _ => false,
    }
}

/// When both owners are packed arrays with comparable structure, compare
/// columns (and CSR maps) without Point materialization. `None` means fall
/// back to the dual stream (mixed / masked / cross-kind).
fn try_columnar_storage_equal(left: &CoordinateOwner, right: &CoordinateOwner) -> Option<bool> {
    match (left, right) {
        (CoordinateOwner::Shape(a), CoordinateOwner::Shape(b)) => shape_columnar_equal(a, b),
        (
            CoordinateOwner::Array {
                storage: sa,
                missing: None,
            },
            CoordinateOwner::Array {
                storage: sb,
                missing: None,
            },
        ) => packed_columnar_equal(sa, sb),
        _ => None,
    }
}

fn shape_columnar_equal(left: &ShapeData, right: &ShapeData) -> Option<bool> {
    match (left.shape(), right.shape()) {
        (Shape::LineString(la), Shape::LineString(lb)) => {
            Some(coordseq_columns_bit_eq(la.as_coords(), lb.as_coords()))
        },
        (Shape::MultiPoint(la), Shape::MultiPoint(lb)) => Some(coordseq_columns_bit_eq(la, lb)),
        (Shape::Point(pa), Shape::Point(pb)) => Some(pa == pb),
        (Shape::Empty(ka, aa), Shape::Empty(kb, ab)) => Some(ka == kb && aa == ab),
        // Multipart / polygon: fall back to dual stream.
        _ => None,
    }
}

fn packed_columnar_equal(
    left: &GeometryArrayStorage,
    right: &GeometryArrayStorage,
) -> Option<bool> {
    match (left, right) {
        (
            GeometryArrayStorage::Points {
                coords: ca,
                row_map: ma,
            },
            GeometryArrayStorage::Points {
                coords: cb,
                row_map: mb,
            },
        ) => packed_points_columnar_eq(ca, ma, cb, mb),
        (
            GeometryArrayStorage::Lines {
                coords: ca,
                offsets: oa,
                row_map: ma,
            },
            GeometryArrayStorage::Lines {
                coords: cb,
                offsets: ob,
                row_map: mb,
            },
        ) => packed_lines_columnar_eq(ca, oa.as_slice(), ma, cb, ob.as_slice(), mb),
        (
            GeometryArrayStorage::Polygons {
                coords: ca,
                ring_offsets: ra,
                polygon_offsets: pa,
                row_map: ma,
            },
            GeometryArrayStorage::Polygons {
                coords: cb,
                ring_offsets: rb,
                polygon_offsets: pb,
                row_map: mb,
            },
        ) => packed_polygons_columnar_eq(
            ca,
            ra.as_slice(),
            pa.as_slice(),
            ma,
            cb,
            rb.as_slice(),
            pb.as_slice(),
            mb,
        ),
        // Mixed or cross-kind: stream.
        _ => None,
    }
}

fn packed_points_columnar_eq(
    ca: &CoordSeq,
    ma: &crate::array::RowSelection,
    cb: &CoordSeq,
    mb: &crate::array::RowSelection,
) -> Option<bool> {
    // Cross row-map pairs (identity vs window/gather, gather vs rebuild) can
    // still be value-equal — never answer False solely from map shape.
    if !row_selection_eq(ma, mb) {
        return None;
    }
    match ma.as_deref() {
        crate::array::RowSelectionRef::Identity => Some(coordseq_columns_bit_eq(ca, cb)),
        crate::array::RowSelectionRef::Window { start, len } => {
            let la = ca.view(CoordWindow::trusted(start..start + len, ca.len()));
            let lb = cb.view(CoordWindow::trusted(start..start + len, cb.len()));
            Some(coordseq_columns_bit_eq(&la, &lb))
        },
        // Gathered points: physical rows may differ — stream is the safe path.
        crate::array::RowSelectionRef::Gather(_) => None,
    }
}

/// Flattened `Coordinates` equality cares about the visible vertex stream only.
/// CSR row/ring breaks do not change flatten order when both sides walk the
/// same packed column in row order — never require absolute offsets to match.
fn packed_lines_columnar_eq(
    ca: &CoordSeq,
    oa: &[i32],
    ma: &crate::array::RowSelection,
    cb: &CoordSeq,
    ob: &[i32],
    mb: &crate::array::RowSelection,
) -> Option<bool> {
    if !row_selection_eq(ma, mb) {
        return None;
    }
    match ma.as_deref() {
        // Identity: flattened stream IS the coord columns (offsets only
        // partition rows; different breaks with the same verts stay equal).
        crate::array::RowSelectionRef::Identity => Some(coordseq_columns_bit_eq(ca, cb)),
        crate::array::RowSelectionRef::Window { start, len } => {
            let end = start + len;
            if oa.len() <= end || ob.len() <= end {
                return Some(false);
            }
            // Per-side absolute spans (prefixes may differ); compare only the
            // visible coordinate slices, never the absolute offset numbers.
            let c0a = oa[start] as usize;
            let c1a = oa[end] as usize;
            let c0b = ob[start] as usize;
            let c1b = ob[end] as usize;
            if c1a > ca.len() || c1b > cb.len() || c0a > c1a || c0b > c1b {
                return Some(false);
            }
            let la = ca.view(CoordWindow::trusted(c0a..c1a, ca.len()));
            let lb = cb.view(CoordWindow::trusted(c0b..c1b, cb.len()));
            Some(coordseq_columns_bit_eq(&la, &lb))
        },
        crate::array::RowSelectionRef::Gather(_) => None,
    }
}

fn packed_polygons_columnar_eq(
    ca: &CoordSeq,
    _ra: &[i32],
    pa: &[i32],
    ma: &crate::array::RowSelection,
    cb: &CoordSeq,
    _rb: &[i32],
    pb: &[i32],
    mb: &crate::array::RowSelection,
) -> Option<bool> {
    if !row_selection_eq(ma, mb) {
        return None;
    }
    match ma.as_deref() {
        // Same as lines: flatten order is the packed coord column; CSR only
        // partitions rings/polygons.
        crate::array::RowSelectionRef::Identity => Some(coordseq_columns_bit_eq(ca, cb)),
        crate::array::RowSelectionRef::Window { start, len } => {
            // Visible polygons start..end → ring range via polygon_offsets →
            // coord span via ring_offsets. Different absolute prefixes are fine.
            let end = start + len;
            if pa.len() <= end || pb.len() <= end {
                return None;
            }
            // Fall through to dual stream for windowed polygons: resolving the
            // two-level CSR span without duplicating ring-offset wiring is the
            // dual stream's job. Identity (the hot full-array case) is bulk.
            let _ = (pa, pb, end);
            None
        },
        crate::array::RowSelectionRef::Gather(_) => None,
    }
}

/// Visible ordinate fingerprint used by layout-aware equality (NaN-stable).
fn visible_coordinate_bits(
    point: Point,
    layout: Option<CoordinateAxes>,
) -> ([Option<u64>; 4], usize) {
    let axes = layout.unwrap_or_else(|| CoordinateAxes::from_point(point));
    let mut values = [None; 4];
    values[0] = Some(point.x.to_bits());
    values[1] = Some(point.y.to_bits());
    let mut n = 2;
    if axes.has_z() {
        values[n] = point.z().map(f64::to_bits);
        n += 1;
    }
    if axes.has_m() {
        values[n] = point.m().map(f64::to_bits);
        n += 1;
    }
    (values, n)
}

impl CoordinateView {
    fn gather_y(&self) -> Vec<f64> {
        let mut out = Vec::with_capacity(self.flattened_capacity());
        self.for_each(&mut |_, _, _, run| match run {
            CoordinateRun::Seq(seq) => out.extend_from_slice(seq.ys()),
            CoordinateRun::Point(point) => out.push(point.y),
        });
        out
    }

    fn gather_optional(&self, project: impl Fn(Point) -> Option<f64>) -> CoordinateColumnRef<'_> {
        // One pass into a dense f64 lane + 1-byte presence mask (≈9 B/coord).
        // Homogeneous present → Dense (no Option). Fully absent → Missing.
        // Mixed → Nullable once at the end so list paths keep None for absent
        // ordinates while still preserving Some(NaN) as a real measured value
        // (a NaN-only lane would collapse the two). Peak is the dense+mask
        // working set, not a 16 B Option lane overlapping a dense prefix.
        let capacity = self.flattened_capacity();
        let mut values = Vec::with_capacity(capacity);
        let mut present = Vec::with_capacity(capacity);
        let mut any = false;
        let mut all_present = true;
        let mut push = |point: Point| {
            if let Some(value) = project(point) {
                any = true;
                values.push(value);
                present.push(true);
            } else {
                all_present = false;
                // Placeholder; only read when `present` is true.
                values.push(0.0);
                present.push(false);
            }
        };
        self.for_each(&mut |_, _, _, run| match run {
            CoordinateRun::Seq(seq) => seq.iter().for_each(&mut push),
            CoordinateRun::Point(point) => push(point),
        });
        if !any {
            return CoordinateColumnRef::Missing;
        }
        if all_present {
            return CoordinateColumnRef::Dense(values);
        }
        CoordinateColumnRef::Nullable(
            values
                .into_iter()
                .zip(present)
                .map(|(value, is_present)| is_present.then_some(value))
                .collect(),
        )
    }
}

/// O(log rows) flat-index lookup into packed line CSR under identity or a
/// contiguous row window. Gather falls through (non-contiguous coord spans).
fn packed_line_point_at(
    coords: &CoordSeq,
    offsets: &[i32],
    map: crate::array::RowSelectionRef<'_>,
    index: usize,
) -> Option<CoordinatePoint> {
    if offsets.len() < 2 {
        return None;
    }
    let (row_start, row_len, coord_base) = match map {
        crate::array::RowSelectionRef::Identity => {
            let total = offsets[offsets.len() - 1] as usize;
            if index >= total {
                return None;
            }
            (0_usize, offsets.len() - 1, 0_usize)
        },
        crate::array::RowSelectionRef::Window { start, len } => {
            if len == 0 {
                return None;
            }
            let coord_start = offsets[start] as usize;
            let coord_end = offsets[start + len] as usize;
            if index >= coord_end - coord_start {
                return None;
            }
            (start, len, coord_start)
        },
        crate::array::RowSelectionRef::Gather(_) => return None,
    };
    let physical_coord = coord_base + index;
    // Binary-search the physical row whose CSR window contains the coord.
    let physical_row = offsets[row_start..=row_start + row_len]
        .partition_point(|&off| (off as usize) <= physical_coord)
        .saturating_sub(1)
        .min(row_len.saturating_sub(1))
        + row_start;
    let row_coord_start = offsets[physical_row] as usize;
    let logical_row = physical_row - row_start;
    Some(CoordinatePoint {
        point: coords.point_at(physical_coord),
        path: CoordinatePath {
            geometry: Some(logical_row),
            part: 0,
            ring: None,
            coord: physical_coord - row_coord_start,
        },
    })
}

/// O(log rings) flat-index lookup into packed polygon CSR under identity or a
/// contiguous row window.
fn packed_polygon_point_at(
    coords: &CoordSeq,
    rings: &[i32],
    polys: &[i32],
    map: crate::array::RowSelectionRef<'_>,
    index: usize,
) -> Option<CoordinatePoint> {
    if rings.len() < 2 || polys.len() < 2 {
        return None;
    }
    let (row_start, row_len, coord_base) = match map {
        crate::array::RowSelectionRef::Identity => {
            let total = rings[rings.len() - 1] as usize;
            if index >= total {
                return None;
            }
            (0_usize, polys.len() - 1, 0_usize)
        },
        crate::array::RowSelectionRef::Window { start, len } => {
            if len == 0 {
                return None;
            }
            let first_ring = polys[start] as usize;
            let last_ring = polys[start + len] as usize;
            let coord_start = rings[first_ring] as usize;
            let coord_end = rings[last_ring] as usize;
            if index >= coord_end - coord_start {
                return None;
            }
            (start, len, coord_start)
        },
        crate::array::RowSelectionRef::Gather(_) => return None,
    };
    let physical_coord = coord_base + index;
    let ring = rings
        .partition_point(|&off| (off as usize) <= physical_coord)
        .saturating_sub(1)
        .min(rings.len() - 2);
    let ring_start = rings[ring] as usize;
    // Restrict the polygon binary search to the selected row window.
    let poly_lo = row_start;
    let poly_hi = row_start + row_len;
    let physical_row = polys[poly_lo..=poly_hi]
        .partition_point(|&off| (off as usize) <= ring)
        .saturating_sub(1)
        .min(row_len.saturating_sub(1))
        + poly_lo;
    let row_ring0 = polys[physical_row] as usize;
    let logical_row = physical_row - row_start;
    Some(CoordinatePoint {
        point: coords.point_at(physical_coord),
        path: CoordinatePath {
            geometry: Some(logical_row),
            part: 0,
            ring: Some(ring - row_ring0),
            coord: physical_coord - ring_start,
        },
    })
}

/// Breakable shape walk — invoke `visit(geometry, part, ring, run)` for each
/// contiguous sequence or scalar point of `shape` in canonical flatten order.
/// Stops when `visit` returns `ControlFlow::Break`. `geometry` is the array
/// row when the owner is a `GeometryArray`.
fn try_walk<B, F>(shape: &Shape, geometry: Option<usize>, visit: &mut F) -> ControlFlow<B>
where
    F: for<'x> FnMut(Option<usize>, usize, Option<usize>, CoordinateRun<'x>) -> ControlFlow<B>,
{
    match shape {
        Shape::Point(point) => visit(geometry, 0, None, CoordinateRun::Point(*point)),
        Shape::MultiPoint(seq) => visit(geometry, 0, None, CoordinateRun::Seq(seq)),
        Shape::LineString(seq) => visit(geometry, 0, None, CoordinateRun::Seq(seq)),
        Shape::MultiLineString(lines) => {
            for (part, line) in lines.iter().enumerate() {
                visit(geometry, part, None, CoordinateRun::Seq(line))?;
            }
            ControlFlow::Continue(())
        },
        Shape::Polygon(polygon) => {
            visit(
                geometry,
                0,
                Some(0),
                CoordinateRun::Seq(polygon.shell.coords()),
            )?;
            for (ring, hole) in polygon.holes.iter().enumerate() {
                visit(
                    geometry,
                    0,
                    Some(ring + 1),
                    CoordinateRun::Seq(hole.coords()),
                )?;
            }
            ControlFlow::Continue(())
        },
        Shape::MultiPolygon(polygons) => {
            for (part, polygon) in polygons.iter().enumerate() {
                visit(
                    geometry,
                    part,
                    Some(0),
                    CoordinateRun::Seq(polygon.shell.coords()),
                )?;
                for (ring, hole) in polygon.holes.iter().enumerate() {
                    visit(
                        geometry,
                        part,
                        Some(ring + 1),
                        CoordinateRun::Seq(hole.coords()),
                    )?;
                }
            }
            ControlFlow::Continue(())
        },
        Shape::GeometryCollection(geometries) => {
            for geom in geometries {
                try_walk(geom, geometry, visit)?;
            }
            ControlFlow::Continue(())
        },
        Shape::Empty(..) => ControlFlow::Continue(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{MissingMask, RowSelection};
    use crate::geometry::{
        CoordSeq, CsrOffsetColumn, LineSeq, Polygon, PolygonLevel, Ring, RingLevel,
    };
    use crate::{Frame, PyGeometryArray};

    fn pt(x: f64, y: f64) -> Point {
        Point::new(x, y).unwrap()
    }

    fn line(coords: &[(f64, f64)]) -> Arc<ShapeData> {
        Arc::new(ShapeData::new(Shape::LineString(
            LineSeq::try_new(CoordSeq::from(
                coords.iter().map(|&(x, y)| pt(x, y)).collect::<Vec<_>>(),
            ))
            .expect("test line is valid"),
        )))
    }

    #[test]
    fn point_view_is_one_coordinate() {
        let view = CoordinateView::from_shape(Arc::new(ShapeData::new(Shape::Point(pt(1.0, 2.0)))));
        assert_eq!(view.len(), 1);
        assert!(view.single_seq().is_none());
        assert_eq!(view.point_at(0).unwrap().point, pt(1.0, 2.0));
        assert!(view.point_at(1).is_none());
    }

    #[test]
    fn linestring_view_is_zero_copy() {
        let view = CoordinateView::from_shape(line(&[(0.0, 0.0), (1.0, 2.0), (3.0, 4.0)]));
        assert_eq!(view.len(), 3);
        let seq = view.single_seq().expect("one contiguous run");
        assert_eq!(seq.xs(), &[0.0, 1.0, 3.0]);
        assert_eq!(seq.ys(), &[0.0, 2.0, 4.0]);
        assert!(matches!(
            view.column(CoordinateAxis::X),
            CoordinateColumnRef::Borrowed(_)
        ));
        assert!(matches!(
            view.column(CoordinateAxis::Z),
            CoordinateColumnRef::Missing
        ));
    }

    #[test]
    fn polygon_view_flattens_shell_then_holes() {
        let shell =
            Ring::closed(vec![pt(0.0, 0.0), pt(4.0, 0.0), pt(4.0, 4.0), pt(0.0, 4.0)]).unwrap();
        let hole =
            Ring::closed(vec![pt(1.0, 1.0), pt(2.0, 1.0), pt(2.0, 2.0), pt(1.0, 2.0)]).unwrap();
        let view = CoordinateView::from_shape(Arc::new(ShapeData::new(Shape::Polygon(Polygon {
            shell,
            holes: vec![hole].into(),
        }))));
        assert!(view.single_seq().is_none());
        let points = view.collect_points();
        assert_eq!(points.len(), view.len());
        assert_eq!(points[0].path.ring, Some(0));
        assert_eq!(points[points.len() - 1].path.ring, Some(1));
        // X column over the multipart polygon materializes (no single run).
        assert!(matches!(
            view.column(CoordinateAxis::X),
            CoordinateColumnRef::Dense(_)
        ));
    }

    #[test]
    fn packed_polygon_view_walks_csr_rows_and_rings_in_order() {
        let coords = CoordSeq::from(vec![
            pt(0.0, 0.0),
            pt(4.0, 0.0),
            pt(4.0, 4.0),
            pt(0.0, 4.0),
            pt(0.0, 0.0),
            pt(1.0, 1.0),
            pt(2.0, 1.0),
            pt(2.0, 2.0),
            pt(1.0, 2.0),
            pt(1.0, 1.0),
            pt(10.0, 10.0),
            pt(11.0, 10.0),
            pt(11.0, 11.0),
            pt(10.0, 11.0),
            pt(10.0, 10.0),
        ]);
        let ring_offsets = CsrOffsetColumn::<RingLevel>::try_new(vec![0, 5, 10, 15], 15).unwrap();
        let polygon_offsets = CsrOffsetColumn::<PolygonLevel>::try_new(vec![0, 2, 3], 3).unwrap();
        let array = PyGeometryArray::packed_polygons(
            coords.clone(),
            ring_offsets.clone(),
            polygon_offsets.clone(),
            Frame::default(),
        );
        let points = array.coordinate_view().collect_points();

        assert_eq!(points.len(), 15);
        assert_eq!(points[0].path.geometry, Some(0));
        assert_eq!(points[0].path.ring, Some(0));
        assert_eq!(points[5].path.geometry, Some(0));
        assert_eq!(points[5].path.ring, Some(1));
        assert_eq!(points[10].path.geometry, Some(1));
        assert_eq!(points[10].path.ring, Some(0));
        assert_eq!(points[14].path.coord, 4);

        let gathered = PyGeometryArray::packed_polygons_mapped(
            coords,
            ring_offsets,
            polygon_offsets,
            Frame::default(),
            RowSelection::gather_trusted(vec![1, 0].into(), 2),
        );
        let gathered_points = gathered.coordinate_view().collect_points();
        assert_eq!(gathered_points.len(), 15);
        assert_eq!(gathered_points[0].point, pt(10.0, 10.0));
        assert_eq!(gathered_points[0].path.geometry, Some(0));
        assert_eq!(gathered_points[5].point, pt(0.0, 0.0));
        assert_eq!(gathered_points[5].path.geometry, Some(1));
        assert_eq!(gathered_points[10].path.ring, Some(1));

        let masked = gathered.with_missing_mask(MissingMask::from_vec(2, vec![true, false]));
        let masked_view = masked.coordinate_view();
        let masked_points = masked_view.collect_points();
        assert_eq!(masked_view.flattened_capacity(), 10);
        assert_eq!(masked_points.len(), 10);
        assert!(
            masked_points
                .iter()
                .all(|point| point.path.geometry == Some(1))
        );
        assert_eq!(masked_points[0].path.ring, Some(0));
        assert_eq!(masked_points[5].path.ring, Some(1));
    }

    #[test]
    fn identity_packed_lines_row_index_fills_from_csr_offsets() {
        let coords = CoordSeq::from(vec![
            pt(0.0, 0.0),
            pt(1.0, 0.0),
            pt(2.0, 0.0),
            pt(3.0, 0.0),
            pt(4.0, 0.0),
        ]);
        let offsets = CsrOffsetColumn::try_new(vec![0, 2, 5], 5).unwrap();
        let array = PyGeometryArray::packed_lines(coords, offsets, Frame::default());
        let view = array.coordinate_view();
        assert_eq!(view.row_index_i64(), vec![0, 0, 1, 1, 1]);
        assert_eq!(view.row_index(), vec![0, 0, 1, 1, 1]);
        assert!(view.single_seq().is_some());
    }

    #[test]
    fn identity_packed_polygons_row_index_spans_all_rings() {
        let coords = CoordSeq::from(vec![
            pt(0.0, 0.0),
            pt(4.0, 0.0),
            pt(4.0, 4.0),
            pt(0.0, 4.0),
            pt(0.0, 0.0),
            pt(1.0, 1.0),
            pt(2.0, 1.0),
            pt(2.0, 2.0),
            pt(1.0, 2.0),
            pt(1.0, 1.0),
            pt(10.0, 10.0),
            pt(11.0, 10.0),
            pt(11.0, 11.0),
            pt(10.0, 11.0),
            pt(10.0, 10.0),
        ]);
        let ring_offsets = CsrOffsetColumn::<RingLevel>::try_new(vec![0, 5, 10, 15], 15).unwrap();
        let polygon_offsets = CsrOffsetColumn::<PolygonLevel>::try_new(vec![0, 2, 3], 3).unwrap();
        let array = PyGeometryArray::packed_polygons(
            coords,
            ring_offsets,
            polygon_offsets,
            Frame::default(),
        );
        let view = array.coordinate_view();
        // Row 0: shell (5) + hole (5); row 1: shell (5).
        assert_eq!(view.row_index_i64(), vec![
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1
        ]);
    }

    #[test]
    fn gathered_packed_lines_row_index_uses_logical_rows() {
        let coords = CoordSeq::from(vec![
            pt(0.0, 0.0),
            pt(1.0, 0.0),
            pt(2.0, 0.0),
            pt(3.0, 0.0),
            pt(4.0, 0.0),
            pt(5.0, 0.0),
        ]);
        let offsets = CsrOffsetColumn::try_new(vec![0, 2, 4, 6], 6).unwrap();
        let gathered = PyGeometryArray::packed_lines_mapped(
            coords,
            offsets,
            Frame::default(),
            RowSelection::gather_trusted(vec![2, 0].into(), 3),
        );
        let view = gathered.coordinate_view();
        // Logical row 0 = physical 2 (2 verts); logical row 1 = physical 0 (2 verts).
        assert_eq!(view.row_index_i64(), vec![0, 0, 1, 1]);
        let points = view.collect_points();
        assert_eq!(points[0].point, pt(4.0, 0.0));
        assert_eq!(points[2].point, pt(0.0, 0.0));
    }

    #[test]
    fn collection_flattens_in_declaration_order() {
        let collection = Shape::GeometryCollection(vec![
            Shape::Point(pt(9.0, 9.0)),
            Shape::LineString(
                LineSeq::try_new(CoordSeq::from(vec![pt(0.0, 0.0), pt(1.0, 1.0)]))
                    .expect("test line is valid"),
            ),
        ]);
        let view = CoordinateView::from_shape(Arc::new(ShapeData::new(collection)));
        assert_eq!(view.len(), 3);
        let points = view.collect_points();
        assert_eq!(points[0].point, pt(9.0, 9.0));
        assert_eq!(points[1].point, pt(0.0, 0.0));
        assert_eq!(points[2].point, pt(1.0, 1.0));
        assert_eq!(view.row_index(), vec![0, 0, 0]);
    }

    #[test]
    fn optional_z_preserves_absent_vs_nan() {
        // Mixed Z presence must keep None for axes-absent vertices and
        // Some(NaN) for a measured NaN — the list path needs the distinction,
        // while NumPy maps both to NaN at the boundary.
        use crate::geometry::{MOrdinate, ZOrdinate};
        let collection = Shape::GeometryCollection(vec![
            Shape::Point(pt(0.0, 0.0)),
            Shape::Point(Point::new_unchecked_axes(
                1.0,
                1.0,
                ZOrdinate(Some(f64::NAN)),
                MOrdinate(None),
            )),
            Shape::Point(
                Point::new_axes(2.0, 2.0, ZOrdinate(Some(9.0)), MOrdinate(None))
                    .expect("finite test Z"),
            ),
        ]);
        let view = CoordinateView::from_shape(Arc::new(ShapeData::new(collection)));
        match view.column(CoordinateAxis::Z) {
            CoordinateColumnRef::Nullable(values) => {
                assert_eq!(values.len(), 3);
                assert_eq!(values[0], None);
                assert!(values[1].is_some_and(f64::is_nan));
                assert_eq!(values[2], Some(9.0));
            },
            CoordinateColumnRef::Missing => panic!("expected Nullable Z column, got Missing"),
            CoordinateColumnRef::Borrowed(_) => panic!("expected Nullable Z column, got Borrowed"),
            CoordinateColumnRef::Dense(_) => panic!("expected Nullable Z column, got Dense"),
        }
    }
}
