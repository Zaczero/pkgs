#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::geometry::{
    Bounds, Coordinates, Orientation, Point, Shape, XY, orientation, shell_is_convex,
};
/// orientation sign — no raycasts, no extent checks. Decisive BOTH ways
/// for `covers`: a vertex strictly outside some halfplane is off the
/// container; all vertices passing puts the candidate's hull (hence the
/// candidate) inside the closure.
/// `STRICT` demands every vertex strictly inside every halfplane — for
/// an OPEN convex region the hull of strictly-interior points is
/// strictly interior, so this settles `contains_properly` for ANY
/// candidate (a boundary-touching vertex already violates it).
pub(crate) fn convex_halfplanes_cover<const STRICT: bool, C: Coordinates + ?Sized>(
    shell: &C,
    ccw: bool,
    candidate: &Shape,
) -> bool {
    let (inside, outside) = if ccw {
        (Orientation::CounterClockwise, Orientation::Clockwise)
    } else {
        (Orientation::Clockwise, Orientation::CounterClockwise)
    };
    // Visitor-based (no per-pair vertex buffer): the first violating
    // vertex short-circuits.
    !candidate.any_point(|probe| {
        !shell.segment_pairs().all(|[start, end]| {
            let turn = orientation(start, end, probe);
            if STRICT {
                turn == inside
            } else {
                turn != outside
            }
        })
    })
}

/// Whether the axis-aligned box sits STRICTLY inside the convex shell —
/// the subtree-acceptance test of the index containment descent (an
/// open convex region containing all four corners contains the box).
pub(crate) fn convex_box_strictly_inside<C: Coordinates + ?Sized>(
    shell: &C,
    ccw: bool,
    bounds: [f64; 4],
) -> bool {
    let inside = if ccw {
        Orientation::CounterClockwise
    } else {
        Orientation::Clockwise
    };
    let [minx, miny, maxx, maxy] = bounds;
    [
        XY::new(minx, miny),
        XY::new(maxx, miny),
        XY::new(maxx, maxy),
        XY::new(minx, maxy),
    ]
    .into_iter()
    .all(|corner| {
        shell
            .segment_pairs()
            .all(|[start, end]| orientation(start, end, corner) == inside)
    })
}

/// Whether `container` is a convex hole-free polygon whose closure holds
/// EVERY vertex of `candidate` — callable only right after a FAILED
/// `vertex_witness` uncovered-vertex refutation, whose probe budget must
/// therefore have reached every vertex. By convexity the candidate's
/// hull (hence the candidate itself) lies within the container.
pub(crate) fn convex_covers_all_vertices(container: &Shape, candidate: &Shape) -> bool {
    let Shape::Polygon(polygon) = container else {
        return false;
    };
    polygon.holes.is_empty()
        && candidate.coord_count() <= PROBE_LIMIT
        && shell_is_convex(polygon.shell.coords())
}

pub(crate) const PROBE_LIMIT: usize = 32;

/// Whether any of the first vertices of `shape` (probe budget capped)
/// satisfies `test` — the overlaps witness scan.
/// Visitor-based: no per-call vertex buffer (this runs once per PAIR in
/// the batch lanes).
pub(crate) fn vertex_witness(shape: &Shape, mut test: impl FnMut(Point) -> bool) -> bool {
    let mut probes = 0_usize;
    let mut hit = false;
    let _ = shape.try_for_each_point(|point| {
        probes += 1;
        if probes > PROBE_LIMIT {
            return Err(());
        }
        if test(point) {
            hit = true;
            return Err(());
        }
        Ok(())
    });
    hit
}

/// Whether the operands' bounds cannot interact (empty operands
/// included) — the shared cheap gate of the relate-backed predicates.
pub(crate) fn option_bounds_disjoint(left: Option<Bounds>, right: Option<Bounds>) -> bool {
    left.zip(right)
        .is_none_or(|(left, right)| !left.intersects(right))
}
