use std::ops::ControlFlow;

use crate::HeapSize;
use crate::geometry::{
    Bounds, Component, CoordSeq, Coordinates as _, IndexedSegment, MonotoneRun, Point, Segment, Shape, XY,
    candidate_pairs_over_staged_runs, same_point, sign_of,
};
/// Flat XY linework staged for the candidate kernel: one segment column,
/// a parallel chain-id column (the kernel's run-break identities), and
/// per-line spans. The per-segment semantic view ([`IndexedSegment`]) is
/// reconstructed in O(1) for the RARE candidate pairs instead of being
/// stored for every segment, and staging is one pass over the raw
/// ordinate columns — no `Point` gathers, no per-segment metadata.
pub(in crate::geometry) struct LineworkChains {
    pub(crate) segments: Vec<Segment>,
    line_of: Vec<u32>,
    spans: Vec<LineSpan>,
    /// Monotone runs and global bounds, built DURING staging (runs break
    /// at line boundaries inherently — each line stages separately), so
    /// the candidate kernel skips its per-call decomposition passes.
    runs: Vec<MonotoneRun>,
    bounds: Bounds,
}

pub(crate) struct LineSpan {
    first: u32,
    count: u32,
    closed: bool,
}

impl Default for LineworkChains {
    fn default() -> Self {
        Self {
            segments: Vec::new(),
            line_of: Vec::new(),
            spans: Vec::new(),
            runs: Vec::new(),
            bounds: Bounds::from_xy_iter(std::iter::empty()),
        }
    }
}

impl LineworkChains {
    /// Append one line/ring column-direct — `None` for a line with no
    /// coordinates at all (the historical early-out the validity paths
    /// rely on).
    pub(crate) fn push_line(&mut self, points: &CoordSeq) -> crate::error::Result<Option<()>> {
        let Some(count) = points.coord_count().checked_sub(1) else {
            return Ok(None);
        };
        let (xs, ys) = (points.xs(), points.ys());
        let closed = count >= 1
            && same_point(
                Point::new_raw_xy(xs[0], ys[0]),
                Point::new_raw_xy(xs[count], ys[count]),
            );
        if self.segments.is_empty() {
            self.bounds = Bounds::from_xy_iter(std::iter::empty());
        }
        let line = u32::try_from(self.spans.len())
            .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
        let first = u32::try_from(self.segments.len())
            .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
        let count_u32 =
            u32::try_from(count).map_err(|_| crate::error::ResourceError::SizeOverflow)?;
        first
            .checked_add(count_u32)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        crate::try_reserve_vec(&mut self.segments, count)?;
        crate::try_reserve_vec(&mut self.line_of, count)?;
        crate::try_reserve_vec(&mut self.spans, 1)?;
        crate::try_reserve_vec(&mut self.runs, count)?;
        let [min_x, min_y, max_x, max_y] = crate::geometry::xy_bounds_columns(xs, ys);
        self.bounds.include_xy(XY::new(min_x, min_y));
        self.bounds.include_xy(XY::new(max_x, max_y));
        // One pass stages segments, bounds, AND the monotone runs (a run
        // extends while the quadrant signs hold; zero-length segments
        // isolate; lines never chain into each other — see the kernel's
        // chain-identity contract).
        let mut run_start = first;
        let (mut sign_x, mut sign_y) = (0_i8, 0_i8);
        // Zero-length segments — the stutter that repeated CONSECUTIVE vertices
        // make — are removable redundancy, not topology, so they are ELIDED
        // here: `A-B-B-C` stages as `A-B-C`. Simplicity/validity therefore never
        // mistake a duplicate vertex for a self-intersection, while every real
        // interaction — bowties,
        // spikes, non-adjacent touches, collinear overlaps, cross-part contact —
        // is built from non-degenerate segments and stays staged. Adjacency and
        // the closed-ring wrap read the STAGED ordinal/count below, so the real
        // segments flanking a duplicate become correctly adjacent.
        let mut staged = 0_u32;
        for index in 0..count {
            let (x0, y0, x1, y1) = (xs[index], ys[index], xs[index + 1], ys[index + 1]);
            let (sx, sy) = (sign_of(x1 - x0), sign_of(y1 - y0));
            if sx == 0 && sy == 0 {
                continue; // zero-length: removable stutter, never staged
            }
            crate::try_push_vec(&mut self.segments, Segment {
                start: Point::new_raw_xy(x0, y0).into(),
                end: Point::new_raw_xy(x1, y1).into(),
            })?;
            crate::try_push_vec(&mut self.line_of, line)?;
            let ordinal = first
                .checked_add(staged)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            staged += 1;
            let chained = ordinal > run_start
                && (sx == 0 || sign_x == 0 || sx == sign_x)
                && (sy == 0 || sign_y == 0 || sy == sign_y);
            if chained {
                if sign_x == 0 {
                    sign_x = sx;
                }
                if sign_y == 0 {
                    sign_y = sy;
                }
                continue;
            }
            self.close_run(run_start, ordinal, sign_x, sign_y)?;
            run_start = ordinal;
            (sign_x, sign_y) = (sx, sy);
        }
        let staged_end = first
            .checked_add(staged)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        self.close_run(run_start, staged_end, sign_x, sign_y)?;
        crate::try_push_vec(&mut self.spans, LineSpan {
            first,
            count: staged,
            closed,
        })?;
        Ok(Some(()))
    }

    fn close_run(
        &mut self,
        start: u32,
        end: u32,
        sign_x: i8,
        sign_y: i8,
    ) -> crate::error::Result<()> {
        if end > start {
            crate::try_push_vec(&mut self.runs, MonotoneRun {
                start,
                end,
                sign_x,
                sign_y,
            })?;
        }
        Ok(())
    }

    /// Run the candidate sweep over the staged linework, reusing the runs and
    /// bounds built during staging so no per-call decomposition pass is needed.
    pub(crate) fn for_each_candidate_pair(
        &self,
        mut visit: impl FnMut(usize, usize) -> ControlFlow<()>,
    ) -> crate::error::Result<ControlFlow<()>> {
        candidate_pairs_over_staged_runs(&self.segments, &self.runs, || self.bounds, &mut visit)
    }

    /// Stage every linework chain of `shape` (lines, ring linework of
    /// polygons, recursively through collections) with a distinct line id
    /// per part, so the adjacency rules read across exactly like
    /// `is_simple`'s (contact between distinct parts always offends).
    pub(crate) fn push_shape(&mut self, shape: &Shape) -> crate::error::Result<()> {
        shape.try_for_each_component(&mut |component| {
            match component {
                Component::LineString(points) => {
                    let _ = self.push_line(points)?;
                },
                Component::Polygon(polygon) => {
                    for ring in polygon.rings() {
                        let _ = self.push_line(ring)?;
                    }
                },
                Component::Point(_) => {},
            }
            Ok(())
        })
    }

    /// The semantic per-segment view, composed on demand.
    pub(crate) fn at(&self, ordinal: usize) -> IndexedSegment {
        let line = self.line_of[ordinal] as usize;
        let span = &self.spans[line];
        IndexedSegment {
            segment: self.segments[ordinal],
            line,
            index: ordinal - span.first as usize,
            count: span.count as usize,
            closed: span.closed,
        }
    }
}

impl HeapSize for LineworkChains {
    fn heap_bytes(&self) -> usize {
        self.segments.heap_bytes()
            + self.line_of.heap_bytes()
            + self.spans.capacity() * std::mem::size_of::<LineSpan>()
            + self.runs.capacity() * std::mem::size_of::<MonotoneRun>()
    }
}
