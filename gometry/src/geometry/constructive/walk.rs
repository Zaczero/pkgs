#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use rstar::AABB;
use rstar::primitives::{GeomWithData, Rectangle};

use crate::geometry::constructive::offset::{MiterConstruction, MiterFinish, miter_join};
use crate::geometry::constructive::{Result, emit_arc, unit_right_normal};
use crate::geometry::{
    AreaSign, Bounds, BufferCapStyle, BufferJoinStyle, BulkRTree, Contact, CoordSeq,
    ExpansionBudget, Ordering, Orientation, Point, Polygon, PolygonPointIndex, Ring, Segment,
    SegmentIndex, SegmentProjection, XY, line_segments, open_xy_cycle_decision, orientation,
    segment_cross_point, segment_projection, segments_intersect, wrap_index,
};

/// One planned join of a right-offset walk, between consecutive offset
/// edges (at the head vertex of the incoming edge).
pub(crate) enum WalkJoin {
    /// Outside turn, round rule: inscribed arc from the incoming normal's
    /// angle, sweeping counter-clockwise.
    Arc { from_angle: f64, sweep: f64 },
    /// Outside turn, miter rule within the limit: the offset carrier
    /// lines' crossing. Pure extension — never consumes an edge, so no
    /// clip parameters.
    Miter { point: XY },
    /// Outside turn, miter rule past the limit: the spike clipped flat
    /// between `entry` (on the incoming carrier) and `exit` (on the
    /// outgoing) at `limit * distance` from the vertex — the GEOS
    /// limited-miter shape, continuous in the corner angle (a bevel
    /// fallback would jump discontinuously as a corner sharpens past the
    /// threshold).
    MiterCut { entry: XY, exit: XY },
    /// Inside turn whose offset edges meet within their extents: the
    /// crossing, with its parameters along the incoming (`t_in`) and
    /// outgoing (`t_out`) offset edges — the edge-consumption evidence
    /// [`WalkPlan::validate`] runs on.
    Cross {
        point: XY,
        t_in: SegmentProjection,
        t_out: SegmentProjection,
    },
    /// Fold fallback: through the original vertex — an excursion lobe the
    /// winding selection cancels where it is an inside fold. Also the
    /// demotion target for any crossing that would traverse an edge backwards.
    Excursion,
    /// Collinear vertex (and bevel outside turns): no join geometry — the
    /// offset endpoints connect directly.
    Straight,
}

impl WalkJoin {
    /// Joins whose own geometry replaces the surrounding offset endpoints
    /// (the incoming edge's end and the outgoing edge's start).
    const fn replaces_endpoints(&self) -> bool {
        matches!(
            self,
            Self::Cross { .. } | Self::Miter { .. } | Self::MiterCut { .. }
        )
    }
}

/// The outside-turn join rule of an offset walk — the style axis of the
/// engine. Inside turns are style-independent (crossings/excursions
/// resolved by the winding selection), exactly as in GEOS.
#[derive(Clone, Copy)]
pub(crate) enum WalkJoinRule {
    Arc,
    Bevel,
    Miter { limit: f64 },
}

impl WalkJoinRule {
    pub(crate) const fn new(join_style: BufferJoinStyle, miter_limit: f64) -> Self {
        match join_style {
            BufferJoinStyle::Round => Self::Arc,
            BufferJoinStyle::Bevel => Self::Bevel,
            BufferJoinStyle::Miter => Self::Miter { limit: miter_limit },
        }
    }
}

/// The crossing of a reflex (inside) join: where the two right-offset
/// edges around the vertex cross, with the crossing's parameters along
/// each edge. `None` when the offset edges do not meet within their
/// extents (deep folds).
pub(crate) fn reflex_cross(
    incoming: Segment,
    outgoing: Segment,
) -> crate::error::Result<Option<(XY, SegmentProjection, SegmentProjection)>> {
    if !segments_intersect(incoming, outgoing) {
        return Ok(None);
    }
    let Some(point) = segment_cross_point(incoming, outgoing) else {
        return Ok(None);
    };
    let t_in = segment_projection(point, incoming)?;
    let t_out = segment_projection(point, outgoing)?;
    debug_assert!(
        t_in.is_start() || t_in.is_end() || (0.0..=1.0).contains(&t_in.fraction())
    );
    debug_assert!(
        t_out.is_start() || t_out.is_end() || (0.0..=1.0).contains(&t_out.fraction())
    );
    Ok(Some((point, t_in, t_out)))
}

/// The outside miter join at `vertex`: the crossing of the two offset
/// carriers when it stays within `limit` offset widths of the vertex,
/// otherwise the spike clipped flat where the perpendicular to the corner
/// bisector at `limit * distance` cuts the two carriers. Near-parallel
/// carriers (no usable crossing) degrade to the direct bevel.
pub(crate) fn convex_miter(
    incoming: Segment,
    outgoing: Segment,
    vertex: Point,
    distance: f64,
    limit: f64,
) -> WalkJoin {
    match miter_join(
        incoming,
        outgoing,
        vertex,
        distance,
        limit,
        MiterFinish::Clip,
    ) {
        MiterConstruction::WithinLimit(point) => WalkJoin::Miter { point },
        MiterConstruction::Clipped { entry, exit } => WalkJoin::MiterCut { entry, exit },
        MiterConstruction::OverLimit | MiterConstruction::Unusable => WalkJoin::Straight,
    }
}

/// A planned right-offset walk over `points`: per-edge unit right normals
/// plus the validated joins between consecutive edges. `joins[k]` sits at
/// the head vertex of edge `k` — cyclic walks carry one join per edge
/// (the last wraps to edge `0`), open walks one fewer. Planning the whole
/// walk before emission lets miters be demoted wherever inversion consumes
/// an entire edge — the cancellation a greedy single-pass emitter breaks.
pub(crate) struct WalkPlan<'a> {
    points: &'a [Point],
    normals: Vec<(f64, f64)>,
    pub(crate) joins: Vec<WalkJoin>,
    cyclic: bool,
    distance: f64,
}

#[cfg(test)]
thread_local! {
    static WALK_PLAN_CONSTRUCTIONS: std::cell::Cell<usize> = const {
        std::cell::Cell::new(0)
    };
}

#[cfg(test)]
pub(crate) fn reset_walk_plan_construction_count() {
    WALK_PLAN_CONSTRUCTIONS.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn walk_plan_construction_count() -> usize {
    WALK_PLAN_CONSTRUCTIONS.with(std::cell::Cell::get)
}

/// Destination for one planned offset-walk vertex.  The walk deliberately
/// owns the duplicate rule, so every destination — count-only preflight,
/// budgeted growth, and materialized columns — sees the identical sequence.
pub(crate) trait WalkSink {
    fn push(&mut self, x: f64, y: f64, source: u32) -> Result<()>;
}

/// Materialized XY columns for a planned walk.  `sources` is present only on
/// the offset-curve path, where generated vertices inherit their corner's
/// Z/M ordinates.
pub(crate) struct WalkColumns {
    xs: Vec<f64>,
    ys: Vec<f64>,
    sources: Option<Vec<u32>>,
}

impl WalkColumns {
    pub(crate) fn try_new(capacity: usize, track_sources: bool) -> Result<Self> {
        let mut xs = Vec::new();
        let mut ys = Vec::new();
        crate::try_reserve_pair(&mut xs, &mut ys, capacity)?;
        let sources = if track_sources {
            let mut sources = Vec::new();
            crate::try_reserve_vec(&mut sources, capacity)?;
            Some(sources)
        } else {
            None
        };
        Ok(Self { xs, ys, sources })
    }

    pub(crate) fn into_columns(self) -> (Vec<f64>, Vec<f64>, Option<Vec<u32>>) {
        (self.xs, self.ys, self.sources)
    }

    fn needs_push(&self, x: f64, y: f64) -> bool {
        !(self
            .xs
            .last()
            .is_some_and(|last| last.to_bits() == x.to_bits())
            && self
                .ys
                .last()
                .is_some_and(|last| last.to_bits() == y.to_bits()))
    }

    fn push_known_new(&mut self, x: f64, y: f64, source: u32) -> Result<()> {
        debug_assert!(self.needs_push(x, y));
        if let Err(error) = crate::try_push_vec(&mut self.xs, x) {
            self.ys = Vec::new();
            self.sources = None;
            return Err(error);
        }
        if let Err(error) = crate::try_push_vec(&mut self.ys, y) {
            self.xs = Vec::new();
            self.sources = None;
            return Err(error);
        }
        if let Some(sources) = &mut self.sources
            && let Err(error) = crate::try_push_vec(sources, source)
        {
            self.xs = Vec::new();
            self.ys = Vec::new();
            self.sources = None;
            return Err(error);
        }
        Ok(())
    }

    pub(crate) const fn len(&self) -> usize {
        self.xs.len()
    }
}

impl WalkSink for WalkColumns {
    fn push(&mut self, x: f64, y: f64, source: u32) -> Result<()> {
        // Exact adjacent duplicates (collinear runs or joins meeting edge
        // offsets bit-for-bit) never reach the loop: every duplicate here is
        // a zero-length noding segment downstream.
        if !self.needs_push(x, y) {
            return Ok(());
        }
        self.push_known_new(x, y, source)
    }
}

/// Count-only companion to [`WalkColumns`].  It retains only the previous
/// vertex because duplicate suppression is part of the emitted-work count.
pub(crate) struct WalkCount<'a> {
    len: usize,
    previous: Option<(u64, u64)>,
    all_finite: bool,
    budget: Option<&'a ExpansionBudget>,
}

impl WalkCount<'_> {
    pub(crate) const fn len(&self) -> usize {
        self.len
    }

    pub(crate) const fn all_finite(&self) -> bool {
        self.all_finite
    }
}

impl<'a> WalkCount<'a> {
    pub(crate) const fn budgeted(budget: &'a ExpansionBudget) -> Self {
        Self {
            len: 0,
            previous: None,
            all_finite: true,
            budget: Some(budget),
        }
    }
}

impl WalkSink for WalkCount<'_> {
    fn push(&mut self, x: f64, y: f64, _: u32) -> Result<()> {
        let point = (x.to_bits(), y.to_bits());
        if self.previous == Some(point) {
            return Ok(());
        }
        self.previous = Some(point);
        let next = self.len.saturating_add(1);
        if let Some(budget) = self.budget {
            budget.check_additional(next)?;
        }
        self.len = next;
        self.all_finite &= x.is_finite() && y.is_finite();
        Ok(())
    }
}

/// Count the sole walk emitter, admit that exact count, then materialize it
/// with an exact final allocation.  The preflight and output are deliberately
/// the same [`WalkPlan::emit_into`] traversal, so arc, crossing, cap, and
/// duplicate rules cannot drift into a second estimate.
pub(crate) fn materialize_walk(
    plan: &WalkPlan<'_>,
    step_angle: f64,
    track_sources: bool,
    budget: &mut ExpansionBudget,
    reject_nonfinite: bool,
) -> Result<Option<WalkColumns>> {
    let count_len = {
        let mut count = WalkCount::budgeted(budget);
        plan.emit_into(step_angle, &mut count)?;
        if !count.all_finite() {
            if reject_nonfinite {
                return Err(crate::geometry::GeometryErrorKind::message(
                    "buffer calculation produced non-finite coordinates",
                ));
            }
            return Ok(None);
        }
        count.len()
    };
    budget.add(count_len)?;
    let mut columns = WalkColumns::try_new(count_len, track_sources)?;
    plan.emit_into(step_angle, &mut columns)?;
    debug_assert_eq!(columns.xs.len(), count_len);
    Ok(Some(columns))
}

/// Close a materialized XY loop at the only point where the duplicate closing
/// coordinate is actually emitted. The walk itself deliberately stays open
/// for winding consumers; direct polygon/offset emitters must charge this
/// final coordinate before growing their result columns.
pub(crate) fn close_xy_loop(
    xs: &mut Vec<f64>,
    ys: &mut Vec<f64>,
    budget: &mut ExpansionBudget,
) -> Result<()> {
    debug_assert_eq!(xs.len(), ys.len());
    let Some((&x, &y)) = xs.first().zip(ys.first()) else {
        return Ok(());
    };
    budget.add(1)?;
    crate::try_push_pair(xs, ys, x, y)
}

impl<'a> WalkPlan<'a> {
    /// Plan and validate one walk. `None` when an edge degenerates or its
    /// offset cannot be represented. A numerically ambiguous near-fold is
    /// emitted as an excursion inside this walk rather than switching to a
    /// different geometry engine.
    pub(crate) fn new(
        points: &'a [Point],
        cyclic: bool,
        distance: f64,
        rule: WalkJoinRule,
    ) -> crate::error::Result<Option<Self>> {
        let count = points.len();
        let edges = if cyclic { count } else { count - 1 };
        let mut normals = Vec::new();
        crate::try_reserve_vec(&mut normals, edges)?;
        for edge in 0..edges {
            let Some(normal) = unit_right_normal(points[edge], points[wrap_index(edge + 1, count)])
            else {
                return Ok(None);
            };
            crate::try_push_vec(&mut normals, normal)?;
        }
        let offset_edge = |from: Point, to: Point, (nx, ny): (f64, f64)| Segment {
            start: Point::new_raw_xy(from.x + distance * nx, from.y + distance * ny).into(),
            end: Point::new_raw_xy(to.x + distance * nx, to.y + distance * ny).into(),
        };
        let join_count = if cyclic { edges } else { edges - 1 };
        let mut joins = Vec::new();
        crate::try_reserve_vec(&mut joins, join_count)?;
        for join in 0..join_count {
            let v = points[join];
            let w = points[wrap_index(join + 1, count)];
            let u = points[wrap_index(join + 2, count)];
            let planned = match orientation(v, w, u) {
                Orientation::CounterClockwise => match rule {
                    WalkJoinRule::Arc => {
                        let (nx, ny) = normals[join];
                        let (mx, my) = normals[wrap_index(join + 1, edges)];
                        let from_angle = ny.atan2(nx);
                        let mut sweep = my.atan2(mx) - from_angle;
                        if sweep < 0.0 {
                            sweep += std::f64::consts::TAU;
                        }
                        if sweep > std::f64::consts::PI {
                            WalkJoin::Excursion
                        } else {
                            WalkJoin::Arc { from_angle, sweep }
                        }
                    },
                    WalkJoinRule::Bevel => WalkJoin::Straight,
                    WalkJoinRule::Miter { limit } => convex_miter(
                        offset_edge(v, w, normals[join]),
                        offset_edge(w, u, normals[wrap_index(join + 1, edges)]),
                        w,
                        distance,
                        limit,
                    ),
                },
                Orientation::Clockwise => match reflex_cross(
                    offset_edge(v, w, normals[join]),
                    offset_edge(w, u, normals[wrap_index(join + 1, edges)]),
                )? {
                    Some((point, t_in, t_out)) => WalkJoin::Cross { point, t_in, t_out },
                    None => WalkJoin::Excursion,
                },
                Orientation::Collinear => WalkJoin::Straight,
            };
            joins.push(planned);
        }
        let mut plan = Self {
            points,
            normals,
            joins,
            cyclic,
            distance,
        };
        plan.validate()?;
        #[cfg(test)]
        WALK_PLAN_CONSTRUCTIONS.with(|count| count.set(count.get() + 1));
        Ok(Some(plan))
    }

    pub(crate) fn first_normal(&self) -> (f64, f64) {
        self.normals
            .first()
            .copied()
            .expect("a walk plan has at least one edge")
    }

    pub(crate) fn last_normal(&self) -> (f64, f64) {
        self.normals
            .last()
            .copied()
            .expect("a walk plan has at least one edge")
    }

    pub(crate) const fn source_len(&self) -> usize {
        self.points.len()
    }

    /// Demote any inside crossing clipping an edge to a BACKWARDS
    /// traversal — entry parameter (the previous join's `t_out`) at or
    /// past the exit (its own join's `t_in`), the signature of an edge
    /// consumed by inversion. The through-vertex excursion is the
    /// always-correct fallback: its lobe cancels in the winding
    /// selection. Outside joins only EXTEND edges (their crossings sit
    /// past the endpoints), so they are never demotion candidates.
    ///
    /// [`reflex_cross`] obtains both parameters from an exact segment contact
    /// and a clamped segment projection, so every `Cross` parameter is in
    /// `[0, 1]`. Demoting a crossing replaces an edge's entry or exit with
    /// the corresponding endpoint, which can only widen that edge's usable
    /// parameter interval. Therefore an already-scanned edge cannot become
    /// backwards later, and one source-ordered scan is the complete fixpoint.
    fn validate(&mut self) -> crate::error::Result<()> {
        let joins = self.joins.len();
        if joins == 0 {
            return Ok(());
        }
        let edges = self.normals.len();
        for edge in 0..edges {
            let entry_join = if edge == 0 {
                self.cyclic.then(|| joins - 1)
            } else {
                Some(edge - 1)
            };
            let exit_join = (edge < joins).then_some(edge);
            let entry = entry_join.and_then(|join| match &self.joins[join] {
                WalkJoin::Cross { t_out, .. } => Some(t_out),
                _ => None,
            });
            let exit = exit_join.and_then(|join| match &self.joins[join] {
                WalkJoin::Cross { t_in, .. } => Some(t_in),
                _ => None,
            });
            let backwards = match (entry, exit) {
                (Some(entry), Some(exit)) => entry.cmp_along(exit)? != Ordering::Less,
                (Some(entry), None) => entry.is_end(),
                (None, Some(exit)) => exit.is_start(),
                (None, None) => false,
            };
            if backwards {
                for join in [entry_join, exit_join].into_iter().flatten() {
                    if matches!(self.joins[join], WalkJoin::Cross { .. }) {
                        self.joins[join] = WalkJoin::Excursion;
                    }
                }
            }
        }
        Ok(())
    }

    /// Append the walk's offset points: per-edge offsets stitched by the
    /// planned joins. Crossings and miters replace BOTH the incoming
    /// edge's end and the outgoing edge's start; on cyclic walks a wrap
    /// join of that kind simply suppresses the first edge's start point
    /// (rotation of a closed loop is free). Cyclic output is the open
    /// column form — the consumer closes the loop.
    /// Emit into one materialized or count-only sink.  There is deliberately
    /// one branch sequence for dry-run planning and realized output: keeping
    /// two similar emitters was the source of the former undercount.
    pub(crate) fn emit_into<S: WalkSink>(&self, step_angle: f64, sink: &mut S) -> Result<()> {
        let count = self.points.len();
        for (edge, &(nx, ny)) in self.normals.iter().enumerate() {
            let previous = if edge == 0 {
                self.cyclic.then(|| self.joins.len() - 1)
            } else {
                Some(edge - 1)
            };
            if !previous.is_some_and(|join| self.joins[join].replaces_endpoints()) {
                let v = self.points[edge];
                sink.push(
                    v.x + self.distance * nx,
                    v.y + self.distance * ny,
                    edge as u32,
                )?;
            }
            let source = wrap_index(edge + 1, count) as u32;
            let w = self.points[source as usize];
            match self.joins.get(edge) {
                Some(WalkJoin::Cross { point, .. } | WalkJoin::Miter { point }) => {
                    sink.push(point.x, point.y, source)?
                },
                Some(WalkJoin::MiterCut { entry, exit }) => {
                    sink.push(entry.x, entry.y, source)?;
                    sink.push(exit.x, exit.y, source)?;
                },
                Some(WalkJoin::Arc { from_angle, sweep }) => {
                    sink.push(w.x + self.distance * nx, w.y + self.distance * ny, source)?;
                    emit_arc(
                        w,
                        *from_angle,
                        *sweep,
                        self.distance,
                        step_angle,
                        source,
                        sink,
                    )?;
                },
                Some(WalkJoin::Excursion) => {
                    sink.push(w.x + self.distance * nx, w.y + self.distance * ny, source)?;
                    sink.push(w.x, w.y, source)?;
                },
                Some(WalkJoin::Straight) | None => {
                    sink.push(w.x + self.distance * nx, w.y + self.distance * ny, source)?;
                },
            }
        }
        Ok(())
    }
}

/// Emit one cap at `tip` continuing from offset normal `(nx, ny)` (the
/// arriving side's normal at the tip): round sweeps pi; flat connects
/// directly (no points — the sides' endpoints already join); square adds
/// the two extended corners.
pub(crate) fn emit_cap<S: WalkSink>(
    tip: Point,
    nx: f64,
    ny: f64,
    distance: f64,
    cap_style: BufferCapStyle,
    step_angle: f64,
    source: u32,
    sink: &mut S,
) -> Result<()> {
    match cap_style {
        BufferCapStyle::Round => {
            emit_arc(
                tip,
                ny.atan2(nx),
                std::f64::consts::PI,
                distance,
                step_angle,
                source,
                sink,
            )?;
        },
        BufferCapStyle::Flat => {},
        BufferCapStyle::Square => {
            // The tip extends along the walk direction, the right-side
            // normal rotated +90 degrees: t = (-ny, nx).
            let (tx, ty) = (-ny, nx);
            sink.push(
                tip.x + distance * (nx + tx),
                tip.y + distance * (ny + ty),
                source,
            )?;
            sink.push(
                tip.x + distance * (-nx + tx),
                tip.y + distance * (-ny + ty),
                source,
            )?;
        },
    }
    Ok(())
}

/// Clean one raw offset loop in its own arrangement and append its
/// surviving `winding <= -1` pieces to `loops` — oriented CW (each piece
/// arrives as a CCW shell from the polygonizer; `reverse` keeps the
/// subtracting orientation the final pass expects). Under erosion the
/// pieces ARE the shrunk-shell lobes and stay in the same CW form.
pub(crate) fn extend_cleaned(
    loops: &mut Vec<(Vec<f64>, Vec<f64>)>,
    raw: &(Vec<f64>, Vec<f64>),
) -> Result<()> {
    for piece in winding_region(std::slice::from_ref(raw), |winding| winding <= -1)? {
        // Shrinking a simply-connected region cannot create nested
        // structure — each surviving piece is a bare shell.
        debug_assert!(piece.holes.is_empty());
        let mut xs = Vec::new();
        let mut ys = Vec::new();
        crate::try_reserve_vec(&mut xs, piece.shell.coords().len())?;
        crate::try_reserve_vec(&mut ys, piece.shell.coords().len())?;
        xs.extend_from_slice(piece.shell.coords().xs());
        ys.extend_from_slice(piece.shell.coords().ys());
        // Drop the closing duplicate and orient CW.
        xs.pop();
        ys.pop();
        xs.reverse();
        ys.reverse();
        crate::try_push_vec(loops, (xs, ys))?;
    }
    Ok(())
}

/// Faces of the noded arrangement of the directed column `loops` whose
/// winding number passes `keep`, re-assembled into nested polygons — the
/// shared region engine behind both of the buffer's winding selections,
/// running on the exact source partition: directed multiplicities give every
/// face's winding from one traversal over the connected exact embedding.
pub(in crate::geometry) fn winding_region(
    loops: &[(Vec<f64>, Vec<f64>)],
    keep: impl Fn(i32) -> bool,
) -> Result<Vec<Polygon>> {
    let inventory = crate::geometry::overlay::ExactSourceInventory::from_raw_loops(loops)?;
    let mut budget = crate::geometry::ExpansionBudget::new("winding_region", "exact partition");
    let partition = inventory.partition(&mut budget)?;
    return inventory.winding_polygons(&partition, |winding| keep(winding as i32));
}



/// Temporary shell owner used while nesting boundary rings. Line coverage is a
/// shell operation: the point index deliberately has no hole state, while the
/// segment index supplies exact boundary contacts for concave shells.
struct ShellCoverageIndex {
    point_index: PolygonPointIndex,
    boundary: SegmentIndex,
}

impl ShellCoverageIndex {
    fn new(shell: &Ring) -> Result<Self> {
        Ok(Self {
            point_index: PolygonPointIndex::build_shell(shell)?,
            boundary: SegmentIndex::build_from_iter(line_segments(shell))?,
        })
    }

    fn covers_xy(&self, x: f64, y: f64) -> bool {
        self.point_index.covers_point(Point::new_raw_xy(x, y))
    }

    /// Boundary-inclusive coverage for a linework chain. Endpoint tests alone
    /// are insufficient for a concave shell: a segment can leave and re-enter
    /// between two covered vertices. The boundary index supplies exact contact
    /// intervals, and one classified midpoint per interval settles each open
    /// portion without constructing a source topology graph.
    fn covers_line(&self, points: &[XY]) -> crate::error::Result<bool> {
        if points.len() < 2 {
            return Ok(true);
        }
        if points.iter().any(|point| !self.covers_xy(point.x, point.y)) {
            return Ok(false);
        }
        for [start, end] in points.array_windows::<2>() {
            let segment = Segment {
                start: *start,
                end: *end,
            };
            let mut contacts = Vec::new();
            for entry in self.boundary.intersecting_candidates(segment) {
                match crate::geometry::segment_contact_exact(segment, entry.segment) {
                    Contact::Disjoint => {},
                    Contact::Cross { .. } => return Ok(false),
                    Contact::Touch { point } => {
                        contacts.push((segment_projection(point, segment)?, point));
                    },
                    Contact::Collinear { span } => {
                        contacts.push((segment_projection(span.start, segment)?, span.start));
                        contacts.push((segment_projection(span.end, segment)?, span.end));
                    },
                }
            }
            contacts.push((
                crate::geometry::segment_projection(*start, segment)?,
                *start,
            ));
            contacts.push((crate::geometry::segment_projection(*end, segment)?, *end));
            crate::geometry::try_sort_by(&mut contacts, |left, right| left.0.cmp_along(&right.0))?;

            let mut previous_projection = &contacts[0].0;
            let mut previous_point = contacts[0].1;
            for (projection, point) in contacts.iter().skip(1) {
                if projection.cmp_along(previous_projection)?.is_eq() {
                    continue;
                }
                let midpoint = crate::geometry::segment_midpoint(Segment {
                    start: previous_point,
                    end: *point,
                });
                if !self.covers_xy(midpoint.x, midpoint.y) {
                    return Ok(false);
                }
                previous_projection = projection;
                previous_point = *point;
            }
        }
        Ok(true)
    }
}

/// Nest boundary rings (shells CCW, holes CW — the
/// exact boundary-walk convention) into polygons: each hole goes
/// to the SMALLEST containing shell.
pub(in crate::geometry) fn assemble_region_polygons(rings: Vec<Vec<XY>>) -> Result<Vec<Polygon>> {
    let mut shells: Vec<(Ring, f64, Bounds)> = Vec::new();
    let mut holes: Vec<Vec<XY>> = Vec::new();
    for ring in rings {
        let decision = open_xy_cycle_decision(&ring);
        let (sign, magnitude) = (decision.sign(), decision.magnitude());
        match sign {
            AreaSign::Positive => {
                let bounds = Bounds::from_xy_iter(ring.iter().copied());
                shells.push((
                    Ring::from_closed(CoordSeq::try_from_xy(&ring)?),
                    magnitude.get(),
                    bounds,
                ));
            },
            AreaSign::Negative => holes.push(ring),
            AreaSign::Zero => {},
        }
    }
    shells.sort_by(|left, right| {
        crate::geometry::cmp_area_keys_then_open_columns(
            left.1,
            right.1,
            left.0.coords().xs(),
            left.0.coords().ys(),
            right.0.coords().xs(),
            right.0.coords().ys(),
        )
    });
    let mut shell_indexes: Vec<Option<ShellCoverageIndex>> =
        std::iter::repeat_with(|| None).take(shells.len()).collect();
    let shell_tree: BulkRTree<GeomWithData<Rectangle<[f64; 2]>, usize>> =
        BulkRTree::bulk_load_with_params(
            shells
                .iter()
                .enumerate()
                .map(|(index, (_, _, bounds))| {
                    GeomWithData::new(
                        Rectangle::from_corners([bounds.minx(), bounds.miny()], [
                            bounds.maxx(),
                            bounds.maxy(),
                        ]),
                        index,
                    )
                })
                .collect(),
        );
    let mut assigned: Vec<Vec<Ring>> = vec![Vec::new(); shells.len()];
    for hole in holes {
        // R-tree traversal has no shell-area order. Restore the sorted position
        // before the exact test so the first result remains the smallest shell.
        let mut candidates: Vec<usize> = hole
            .iter()
            .flat_map(|probe| {
                shell_tree
                    .locate_in_envelope_intersecting(AABB::from_point([probe.x, probe.y]))
                    .map(|entry| entry.data)
            })
            .collect();
        candidates.sort_unstable();
        candidates.dedup();
        // Coverage is boundary-inclusive, so a pinch vertex may lie on the
        // shell. Exact boundary contacts split every hole edge into intervals,
        // which also rejects an edge that leaves and re-enters a concave shell.
        let mut owner = None;
        for shell_index in candidates {
            let index = &mut shell_indexes[shell_index];
            if index.is_none() {
                *index = Some(ShellCoverageIndex::new(&shells[shell_index].0)?);
            }
            if index
                .as_ref()
                .expect("shell index was initialized")
                .covers_line(&hole)?
            {
                owner = Some(shell_index);
                break;
            }
        }
        if let Some(owner) = owner {
            assigned[owner].push(Ring::from_closed(CoordSeq::try_from_xy(&hole)?));
        }
    }
    drop(shell_indexes);
    shells
        .into_iter()
        .zip(assigned)
        .map(|((shell, ..), holes)| Polygon::new(shell, holes))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::{GENERATED_ITEM_LIMIT, Shape};

    fn rectangle(minx: f64, miny: f64, maxx: f64, maxy: f64, clockwise: bool) -> Vec<XY> {
        let mut ring = vec![
            XY::new(minx, miny),
            XY::new(maxx, miny),
            XY::new(maxx, maxy),
            XY::new(minx, maxy),
            XY::new(minx, miny),
        ];
        if clockwise {
            ring.reverse();
        }
        ring
    }

    #[test]
    fn loop_closure_charges_before_appending_the_public_coordinate() {
        let mut budget = ExpansionBudget::new("test", "count");
        budget.add(GENERATED_ITEM_LIMIT).unwrap();
        let mut xs = vec![1.0];
        let mut ys = vec![2.0];

        assert!(close_xy_loop(&mut xs, &mut ys, &mut budget).is_err());
        assert_eq!((xs.len(), ys.len()), (1, 1));
    }

    #[test]
    fn walk_count_is_admitted_before_materializing_the_arc() {
        let points = [
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(1.0, 0.0),
            Point::new_raw_xy(1.0, 1.0),
        ];
        let plan = WalkPlan::new(&points, false, 1.0, WalkJoinRule::Arc)
            .unwrap()
            .unwrap();
        let mut budget = ExpansionBudget::new("test", "count");
        budget.add(GENERATED_ITEM_LIMIT).unwrap();

        assert!(materialize_walk(&plan, 0.1, false, &mut budget, false).is_err());
    }

    #[test]
    fn region_assembly_assigns_holes_to_smallest_shell_after_envelope_filtering() {
        let l_shell = vec![
            XY::new(30.0, 30.0),
            XY::new(70.0, 30.0),
            XY::new(70.0, 40.0),
            XY::new(40.0, 40.0),
            XY::new(40.0, 70.0),
            XY::new(30.0, 70.0),
            XY::new(30.0, 30.0),
        ];
        let pinch_hole = vec![
            XY::new(120.0, 20.0),
            XY::new(130.0, 25.0),
            XY::new(130.0, 15.0),
            XY::new(120.0, 20.0),
        ];
        let polygons = assemble_region_polygons(vec![
            rectangle(0.0, 0.0, 100.0, 100.0, false),
            rectangle(10.0, 10.0, 90.0, 90.0, true),
            rectangle(20.0, 20.0, 80.0, 80.0, false),
            rectangle(30.0, 30.0, 70.0, 70.0, true),
            rectangle(40.0, 40.0, 60.0, 60.0, false),
            rectangle(45.0, 45.0, 55.0, 55.0, true),
            l_shell,
            rectangle(120.0, 0.0, 160.0, 40.0, false),
            pinch_hole,
        ])
        .unwrap();

        let assignments: Vec<((f64, f64), usize)> = polygons
            .iter()
            .map(|polygon| {
                let first = polygon.shell.first().unwrap();
                ((first.x, first.y), polygon.holes.len())
            })
            .collect();
        assert_eq!(assignments, vec![
            ((40.0, 40.0), 1),
            ((30.0, 30.0), 0),
            ((120.0, 0.0), 1),
            ((20.0, 20.0), 1),
            ((0.0, 0.0), 1),
        ]);
    }

    #[test]
    fn region_assembly_rejects_hole_edges_leaving_a_concave_shell() {
        let shell = vec![
            XY::new(0.0, 0.0),
            XY::new(6.0, 0.0),
            XY::new(6.0, 2.0),
            XY::new(2.0, 2.0),
            XY::new(2.0, 6.0),
            XY::new(0.0, 6.0),
            XY::new(0.0, 0.0),
        ];
        let hole = vec![
            XY::new(5.0, 1.0),
            XY::new(1.0, 5.0),
            XY::new(5.5, 1.0),
            XY::new(5.0, 1.0),
        ];
        let shell_shape = Shape::Polygon(
            Polygon::new(
                Ring::from_closed(CoordSeq::try_from_xy(&shell).unwrap()),
                Vec::new(),
            )
            .unwrap(),
        );
        assert!(hole.iter().all(|point| {
            shell_shape
                .covers_point(Point::new_raw_xy(point.x, point.y))
                .unwrap()
        }));
        let polygon = assemble_region_polygons(vec![shell, hole]).unwrap();

        assert_eq!(polygon.len(), 1);
        assert!(polygon[0].holes.is_empty());
    }

    #[test]
    fn indexed_shell_coverage_keeps_boundary_and_rejects_boundary_exit() {
        let shell = vec![
            XY::new(0.0, 0.0),
            XY::new(6.0, 0.0),
            XY::new(6.0, 2.0),
            XY::new(2.0, 2.0),
            XY::new(2.0, 6.0),
            XY::new(0.0, 6.0),
            XY::new(0.0, 0.0),
        ];
        let ring = Ring::from_closed(CoordSeq::try_from_xy(&shell).unwrap());
        let index = ShellCoverageIndex::new(&ring).unwrap();
        assert!(
            index
                .covers_line(&[
                    XY::new(0.0, 0.0),
                    XY::new(6.0, 0.0),
                    XY::new(6.0, 2.0),
                    XY::new(2.0, 2.0),
                ])
                .unwrap()
        );
        assert!(
            !index
                .covers_line(&[XY::new(5.0, 1.0), XY::new(1.0, 5.0), XY::new(5.5, 1.0)])
                .unwrap()
        );
    }

    #[test]
    fn region_assembly_uses_shell_owner_not_holed_polygon_coverage() {
        let shell = rectangle(0.0, 0.0, 10.0, 10.0, false);
        let outer_hole = rectangle(2.0, 2.0, 8.0, 8.0, true);
        let inner_hole = rectangle(3.0, 3.0, 7.0, 7.0, true);
        let holed_shell = Shape::Polygon(
            Polygon::new(
                Ring::from_closed(CoordSeq::try_from_xy(&shell).unwrap()),
                vec![Ring::from_closed(
                    CoordSeq::try_from_xy(&outer_hole).unwrap(),
                )],
            )
            .unwrap(),
        );
        // The second negative ring is inside the first. A completed polygon
        // would exclude it, but assembly owns each negative ring against the
        // shell before those holes become polygon state.
        assert!(inner_hole.iter().all(|point| {
            !holed_shell
                .covers_point(Point::new_raw_xy(point.x, point.y))
                .unwrap()
        }));

        let polygons = assemble_region_polygons(vec![shell, outer_hole, inner_hole]).unwrap();

        assert_eq!(polygons.len(), 1);
        assert_eq!(polygons[0].holes.len(), 2);
    }
}
