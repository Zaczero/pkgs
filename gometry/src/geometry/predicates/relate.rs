use std::ops::ControlFlow;

use crate::geometry::*;
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
    pub(crate) fn push_line(&mut self, points: &CoordSeq) -> Option<()> {
        let count = points.coord_count().checked_sub(1)?;
        let (xs, ys) = (points.xs(), points.ys());
        let closed = count >= 1
            && same_point(
                Point::new_unchecked_xy(xs[0], ys[0]),
                Point::new_unchecked_xy(xs[count], ys[count]),
            );
        if self.segments.is_empty() {
            self.bounds = Bounds::from_xy_iter(std::iter::empty());
        }
        let line = self.spans.len() as u32;
        let first = self.segments.len() as u32;
        self.segments.reserve(count);
        self.line_of.reserve(count);
        // One pass stages segments, bounds, AND the monotone runs (a run
        // extends while the quadrant signs hold; zero-length segments
        // isolate; lines never chain into each other — see the kernel's
        // chain-identity contract).
        let mut run_start = first;
        let (mut sign_x, mut sign_y) = (0_i8, 0_i8);
        // Zero-length segments — the stutter that repeated CONSECUTIVE vertices
        // make — are removable redundancy, not topology, so they are ELIDED
        // here: `A-B-B-C` stages as `A-B-C`. Simplicity/validity therefore never
        // mistake a duplicate vertex for a self-intersection (matching the whole
        // GEOS/JTS/PostGIS ecosystem), while every real interaction — bowties,
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
            self.bounds.include_xy(XY::new(x0, y0));
            self.bounds.include_xy(XY::new(x1, y1));
            self.segments.push(Segment {
                start: Point::new_unchecked_xy(x0, y0).into(),
                end: Point::new_unchecked_xy(x1, y1).into(),
            });
            self.line_of.push(line);
            let ordinal = first + staged;
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
            self.close_run(run_start, ordinal, sign_x, sign_y);
            run_start = ordinal;
            (sign_x, sign_y) = (sx, sy);
        }
        self.close_run(run_start, first + staged, sign_x, sign_y);
        self.spans.push(LineSpan {
            first,
            count: staged,
            closed,
        });
        Some(())
    }

    fn close_run(&mut self, start: u32, end: u32, sign_x: i8, sign_y: i8) {
        if end > start {
            self.runs.push(MonotoneRun {
                start,
                end,
                sign_x,
                sign_y,
            });
        }
    }

    /// Run the candidate sweep over the staged linework: the brute loop
    /// below the pair crossover, otherwise the pre-staged runs/bounds
    /// path (no per-call decomposition).
    pub(crate) fn for_each_candidate_pair(
        &self,
        mut visit: impl FnMut(usize, usize) -> ControlFlow<()>,
    ) -> ControlFlow<()> {
        let count = self.segments.len();
        if count * count < 32 {
            for left in 0..count {
                for right in (left + 1)..count {
                    visit(left, right)?;
                }
            }
            return ControlFlow::Continue(());
        }
        // Runs are pre-staged, so the chain path is profitable WAY below the
        // build-time crossover whenever the linework is smooth (few runs):
        // within a run no pair is ever visited, and a handful of run pairs
        // beat a per-row event sort. Jagged small inputs (runs ~ segments)
        // keep the flat sweep.
        if count < CHAIN_MIN_SEGMENTS && self.runs.len() * 4 > count {
            return flat_segment_sweep(&self.segments, &mut visit);
        }
        candidate_pairs_over_runs(&self.segments, &self.runs, self.bounds, &mut visit)
    }

    /// Stage every linework chain of `shape` (lines, ring linework of
    /// polygons, recursively through collections) with a distinct line id
    /// per part, so the adjacency rules read across exactly like
    /// `is_simple`'s (contact between distinct parts always offends).
    pub(crate) fn push_shape(&mut self, shape: &Shape) {
        match shape {
            Shape::LineString(points) => {
                let _ = self.push_line(points);
            },
            Shape::MultiLineString(lines) => {
                for line in lines {
                    let _ = self.push_line(line);
                }
            },
            Shape::Polygon(polygon) => {
                for ring in polygon.rings() {
                    let _ = self.push_line(ring);
                }
            },
            Shape::MultiPolygon(polygons) => {
                for polygon in polygons {
                    for ring in polygon.rings() {
                        let _ = self.push_line(ring);
                    }
                }
            },
            Shape::GeometryCollection(geometries) => {
                for geometry in geometries {
                    self.push_shape(geometry);
                }
            },
            Shape::Point(_) | Shape::MultiPoint(_) | Shape::Empty(..) => {},
        }
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
