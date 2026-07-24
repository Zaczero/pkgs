use super::*;
use crate::HeapSize;

/// One operand's rings, oriented and de-duplicated ONCE.
///
/// [`oriented_open_ring`] (collect + dedup + signed-area + possible reverse +
/// box) is a pure function of the immutable geometry, yet it dominated the
/// per-relate staging (`push_ring` ≈ 13% of the repeated-relate areal profile).
/// Caching it on `ShapeData` turns every later relate of the same geometry into
/// a cheap segment re-materialization over the already-oriented points.
#[derive(Clone, Debug)]
pub(in crate::geometry) struct StagedRings {
    /// Oriented open point cycles, in operand-contiguous ring order.
    pub(super) rings: Box<[StagedRing]>,
    /// Total directed edges across all rings == total points (each ring is a
    /// closed cycle). The pre-sized pool capacity, computed once.
    pub(super) edge_count: usize,
}

#[derive(Clone, Debug)]
pub(super) struct StagedRing {
    pub(super) polygon: u32,
    pub(super) ring: u32,
    pub(super) is_hole: bool,
    pub(super) points: std::sync::Arc<[XY]>,
    /// Cached representative interior point of the OWNING POLYGON, stored on
    /// the shell ring (`ring == 0`). Computed once here so the relate
    /// interior-face probe is an O(1) lookup instead of a per-call scanline.
    pub(super) probe: Option<XY>,
}

impl StagedRings {
    pub(in crate::geometry) fn heap_bytes(&self) -> usize {
        HeapSize::heap_bytes(self)
    }

    /// Orient every ring of one operand's polygons exactly once. Degenerate
    /// rings (`< 3` distinct vertices) are dropped, mirroring [`push_ring`].
    pub(in crate::geometry) fn build(polygons: &[Polygon]) -> Self {
        let mut rings: Vec<StagedRing> = Vec::new();
        let mut edge_count = 0;
        for (polygon_index, polygon) in polygons.iter().enumerate() {
            let ring_start = rings.len();
            let staged = std::iter::once((0_usize, false, polygon.shell.coords())).chain(
                polygon
                    .holes
                    .iter()
                    .enumerate()
                    .map(|(hole_index, hole)| (hole_index + 1, true, hole.coords())),
            );
            for (ring_index, is_hole, coords) in staged {
                if let Some(points) = oriented_open_ring(coords, is_hole) {
                    edge_count += points.len();
                    rings.push(StagedRing {
                        polygon: polygon_index as u32,
                        ring: ring_index as u32,
                        is_hole,
                        points: points.into(),
                        probe: None,
                    });
                }
            }
            // Cache the polygon's representative interior point on its shell
            // (rings stage shell-first, so the slice opens with the shell).
            let polygon_rings: Vec<&[XY]> = rings[ring_start..]
                .iter()
                .map(|ring| ring.points.as_ref())
                .collect();
            if let Some(probe) = representative_interior_point(&polygon_rings)
                && let Some(shell) = rings[ring_start..].iter_mut().find(|ring| ring.ring == 0)
            {
                shell.probe = Some(probe);
            }
        }
        Self {
            rings: rings.into_boxed_slice(),
            edge_count,
        }
    }
}

impl HeapSize for StagedRing {
    fn heap_bytes(&self) -> usize {
        self.points.heap_bytes()
    }
}

impl HeapSize for StagedRings {
    fn heap_bytes(&self) -> usize {
        self.rings.heap_bytes()
    }
}

/// A representative interior point of a polygon (shell `rings[0]`, holes
/// `rings[1..]`): the midpoint of the first interior span of the bbox-midline
/// scanline — the SAME point the relate interior-face probe would pick, so
/// caching it changes nothing but the cost. `None` for a degenerate polygon.
pub(super) fn representative_interior_point(rings: &[&[XY]]) -> Option<XY> {
    let shell = *rings.first()?;
    let (mut miny, mut maxy) = (f64::INFINITY, f64::NEG_INFINITY);
    for point in shell {
        miny = miny.min(point.y);
        maxy = maxy.max(point.y);
    }
    let y = f64::midpoint(miny, maxy);
    let mut crossings: Vec<f64> = Vec::with_capacity(rings.iter().map(|ring| ring.len()).sum());
    for ring in rings {
        let n = ring.len();
        for index in 0..n {
            let next = wrap_index(index + 1, n);
            let (y0, y1) = (ring[index].y, ring[next].y);
            if (y0 <= y) != (y1 <= y) {
                let t = (y - y0) / (y1 - y0);
                crossings.push(ring[index].x + t * (ring[next].x - ring[index].x));
            }
        }
    }
    if crossings.len() < 2 || !crossings.len().is_multiple_of(2) {
        return None;
    }
    crossings.sort_unstable_by(f64::total_cmp);
    crossings
        .as_chunks::<2>()
        .0
        .iter()
        .find_map(|span| (span[1] > span[0]).then(|| XY::new(f64::midpoint(span[0], span[1]), y)))
}

/// Open point list of a ring oriented so the OPERAND INTERIOR lies to the LEFT
/// of every directed edge. Shells are CCW, holes are CW. The closing duplicate
/// is stripped; degenerate rings (`< 3` distinct vertices) return `None`.
pub(super) fn oriented_open_ring(coords: &CoordSeq, is_hole: bool) -> Option<Box<[XY]>> {
    let (xs, ys) = (coords.xs(), coords.ys());
    let pairs = coords.coord_count().saturating_sub(1);
    let column_ccw = closed_columns_winding(xs, ys, pairs).is_ccw();
    let mut points: Vec<XY> = std::iter::zip(xs, ys)
        .map(|(&x, &y)| XY::new(x, y))
        .collect();
    let staged_len = points.len();
    points.dedup();
    if points.len() > 1 && points[0] == points[points.len() - 1] {
        points.pop();
    }
    if points.len() < Ring::MIN_VERTICES_OPEN {
        return None;
    }
    let ccw = if points.len() < staged_len.saturating_sub(1) {
        // Consecutive-duplicate removal can flip the sign probe — fall back to
        // the staged open-ring kernel on the deduped `XY` list.
        open_xy_cycle_winding(&points).is_ccw()
    } else {
        column_ccw
    };
    if ccw == is_hole {
        points.reverse();
    }
    Some(points.into_boxed_slice())
}
