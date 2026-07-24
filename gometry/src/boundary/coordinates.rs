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
    /// method.
    fn for_each<F>(&self, visit: &mut F)
    where
        F: for<'x> FnMut(Option<usize>, usize, Option<usize>, CoordinateRun<'x>),
    {
        match &self.owner {
            CoordinateOwner::Shape(shape) => walk(shape, None, visit),
            CoordinateOwner::Array { storage, missing } => match storage.as_ref() {
                GeometryArrayStorage::Mixed(items) => {
                    for (row, item) in items.iter().enumerate() {
                        if missing.as_ref().is_some_and(|mask| mask[row]) {
                            continue;
                        }
                        walk(&item.shape, Some(row), visit);
                    }
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
                        );
                    }
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
                        visit(Some(row), 0, None, CoordinateRun::Seq(&view));
                    }
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
                            visit(Some(row), 0, Some(ring), CoordinateRun::Seq(&view));
                        }
                    }
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
                    GeometryArrayStorage::Mixed(items) => items
                        .iter()
                        .map(|item| item.shape.shape().coord_count())
                        .sum(),
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
                GeometryArrayStorage::Mixed(items) => items
                    .iter()
                    .zip(missing.iter())
                    .filter(|(_, missing)| !**missing)
                    .map(|(item, _)| item.shape.shape().coord_count())
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
                GeometryArrayStorage::Mixed(items) => items
                    .iter()
                    .map(|item| item.shape.shape().coord_count())
                    .sum(),
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
            CoordinateOwner::Shape(shape) => shape.shape().coordinate_bytes(),
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
    pub fn for_each_point(&self, mut visit: impl FnMut(CoordinatePoint)) {
        self.for_each(&mut |geometry, part, ring, run| match run {
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
                    });
                }
            },
            CoordinateRun::Point(point) => {
                visit(CoordinatePoint {
                    point,
                    path: CoordinatePath {
                        geometry,
                        part,
                        ring,
                        coord: 0,
                    },
                });
            },
        });
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
        let mut rows = Vec::with_capacity(self.flattened_capacity());
        self.for_each_point(|coord| rows.push(coord.path.geometry.unwrap_or(0)));
        rows
    }

    /// Per-coordinate geometry-row index as NumPy's `int64` lane, built in a
    /// single traversal (no `Vec<usize>` -> `Vec<i64>` remap). Mirrors
    /// `row_index` but emits `i64` directly for the `Coordinates.index` getter.
    pub fn row_index_i64(&self) -> Vec<i64> {
        let mut rows = Vec::with_capacity(self.flattened_capacity());
        self.for_each_point(|coord| rows.push(coord.path.geometry.unwrap_or(0) as i64));
        rows
    }

    /// The coordinate at `index` (flattened), or `None` if out of range.
    pub fn point_at(&self, index: usize) -> Option<CoordinatePoint> {
        let mut remaining = index;
        let mut found = None;
        self.for_each(&mut |geometry, part, ring, run| {
            if found.is_some() {
                return;
            }
            match run {
                CoordinateRun::Seq(seq) => {
                    let len = seq.len();
                    if remaining < len {
                        found = Some(CoordinatePoint {
                            point: seq.point_at(remaining),
                            path: CoordinatePath {
                                geometry,
                                part,
                                ring,
                                coord: remaining,
                            },
                        });
                    } else {
                        remaining -= len;
                    }
                },
                CoordinateRun::Point(point) => {
                    if remaining == 0 {
                        found = Some(CoordinatePoint {
                            point,
                            path: CoordinatePath {
                                geometry,
                                part,
                                ring,
                                coord: 0,
                            },
                        });
                    } else {
                        remaining -= 1;
                    }
                },
            }
        });
        found
    }

    fn gather_x(&self) -> Vec<f64> {
        let mut out = Vec::with_capacity(self.flattened_capacity());
        self.for_each(&mut |_, _, _, run| match run {
            CoordinateRun::Seq(seq) => out.extend_from_slice(seq.xs()),
            CoordinateRun::Point(point) => out.push(point.x),
        });
        out
    }

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

/// The traversal primitive: invoke `visit(geometry, part, ring, run)` for each
/// contiguous sequence or scalar point of `shape`, in canonical flatten order.
/// `geometry` is the array row when the owner is a `GeometryArray`.
fn walk<F>(shape: &Shape, geometry: Option<usize>, visit: &mut F)
where
    F: for<'x> FnMut(Option<usize>, usize, Option<usize>, CoordinateRun<'x>),
{
    match shape {
        Shape::Point(point) => visit(geometry, 0, None, CoordinateRun::Point(*point)),
        Shape::MultiPoint(seq) => {
            visit(geometry, 0, None, CoordinateRun::Seq(seq));
        },
        Shape::LineString(seq) => {
            visit(geometry, 0, None, CoordinateRun::Seq(seq));
        },
        Shape::MultiLineString(lines) => {
            for (part, line) in lines.iter().enumerate() {
                visit(geometry, part, None, CoordinateRun::Seq(line));
            }
        },
        Shape::Polygon(polygon) => {
            visit(
                geometry,
                0,
                Some(0),
                CoordinateRun::Seq(polygon.shell.coords()),
            );
            for (ring, hole) in polygon.holes.iter().enumerate() {
                visit(
                    geometry,
                    0,
                    Some(ring + 1),
                    CoordinateRun::Seq(hole.coords()),
                );
            }
        },
        Shape::MultiPolygon(polygons) => {
            for (part, polygon) in polygons.iter().enumerate() {
                visit(
                    geometry,
                    part,
                    Some(0),
                    CoordinateRun::Seq(polygon.shell.coords()),
                );
                for (ring, hole) in polygon.holes.iter().enumerate() {
                    visit(
                        geometry,
                        part,
                        Some(ring + 1),
                        CoordinateRun::Seq(hole.coords()),
                    );
                }
            }
        },
        Shape::GeometryCollection(geometries) => {
            for geom in geometries {
                walk(geom, geometry, visit);
            }
        },
        Shape::Empty(..) => {},
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
