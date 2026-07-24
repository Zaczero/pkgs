#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;

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
    Cross { point: XY, t_in: f64, t_out: f64 },
    /// Inside turn fallback: through the original vertex — the excursion
    /// lobe the winding selection cancels. Also the demotion target for
    /// any crossing that would traverse a consumed edge backwards.
    Excursion,
    /// Either-direction turn whose two offset endpoints sit within the
    /// separation tolerance (|distance| × 1e-3 — the GEOS
    /// `OffsetSegmentGenerator` economy): ONE snapped point replaces both
    /// endpoints. No arc, no excursion lobe, no ulp-twin endpoint pair
    /// feeding the noding sweep.
    Snap { point: XY },
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
            Self::Cross { .. } | Self::Miter { .. } | Self::MiterCut { .. } | Self::Snap { .. }
        )
    }
}

/// Offset-endpoint separation under which a join SNAPS to one point
/// instead of emitting both endpoints (plus arc/excursion geometry) —
/// GEOS's `OFFSET_SEGMENT_SEPARATION_FACTOR`.
pub(crate) const OFFSET_SEPARATION_FACTOR: f64 = 1e-3;

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
pub(crate) fn reflex_cross(incoming: Segment, outgoing: Segment) -> Option<(XY, f64, f64)> {
    if !segments_intersect(incoming, outgoing) {
        return None;
    }
    let point = segment_cross_point(incoming, outgoing)?;
    Some((
        point,
        segment_projection_fraction(point, incoming),
        segment_projection_fraction(point, outgoing),
    ))
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
    let Some(point) = line_intersection(incoming, outgoing) else {
        return WalkJoin::Straight;
    };
    let (dx, dy) = (point.x - vertex.x, point.y - vertex.y);
    let reach = dx * dx + dy * dy;
    let allowed = limit * distance.abs();
    if reach <= allowed * allowed {
        return WalkJoin::Miter { point };
    }
    // The full miter point sits on the corner's exterior bisector; clip
    // the spike with the bisector's perpendicular at the allowed reach.
    let scale = allowed / reach.sqrt();
    let mid = Point::new_unchecked_xy(vertex.x + dx * scale, vertex.y + dy * scale);
    let clip = Segment {
        start: mid.into(),
        end: Point::new_unchecked_xy(mid.x - dy, mid.y + dx).into(),
    };
    let Some(entry) = line_intersection(incoming, clip) else {
        return WalkJoin::Straight;
    };
    let Some(exit) = line_intersection(outgoing, clip) else {
        return WalkJoin::Straight;
    };
    WalkJoin::MiterCut { entry, exit }
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

impl<'a> WalkPlan<'a> {
    /// Plan and validate one walk. `None` when an edge degenerates or a
    /// left turn sweeps past pi (the robust orientation and the normals
    /// disagree on a near-fold) — the caller falls back to the geo engine.
    pub(crate) fn new(
        points: &'a [Point],
        cyclic: bool,
        distance: f64,
        rule: WalkJoinRule,
        step_angle: f64,
    ) -> Option<Self> {
        let count = points.len();
        let edges = if cyclic { count } else { count - 1 };
        // A convex round join whose turn is no wider than one arc step emits
        // no interior arc vertex (emit_arc's `steps <= 1` early-out), so it
        // renders byte-identically to a Straight join. cos is monotone over
        // the convex sweep range (0, pi], so the unit normals' dot product
        // (= cos sweep) clears `cos(step_angle)` exactly when the sweep is
        // within one step — letting those joins skip the two per-join `atan2`s
        // (measured ~23% of a dense buffer's profile). The +1e-12 margin keeps
        // every arc-emitting join on the exact angular path (FP noise in the
        // dot is ~1e-15), so the output is unchanged.
        let cos_step = step_angle.cos() + 1e-12;
        let normals: Vec<(f64, f64)> = (0..edges)
            .map(|edge| unit_right_normal(points[edge], points[wrap_index(edge + 1, count)]))
            .collect::<Option<_>>()?;
        let offset_edge = |from: Point, to: Point, (nx, ny): (f64, f64)| Segment {
            start: Point::new_unchecked_xy(from.x + distance * nx, from.y + distance * ny).into(),
            end: Point::new_unchecked_xy(to.x + distance * nx, to.y + distance * ny).into(),
        };
        // Two offset endpoints within the separation tolerance carry no
        // usable join geometry: snap to the incoming endpoint (the GEOS
        // economy) — one point instead of endpoint pairs, fillet arcs, or
        // excursion lobes whose extent is below the tolerance anyway.
        let separation = (distance * OFFSET_SEPARATION_FACTOR).abs();
        let snapped = |w: Point, (nx, ny): (f64, f64), (mx, my): (f64, f64)| -> Option<WalkJoin> {
            let dx = distance * (mx - nx);
            let dy = distance * (my - ny);
            (dx * dx + dy * dy <= separation * separation).then(|| WalkJoin::Snap {
                point: XY::new(w.x + distance * nx, w.y + distance * ny),
            })
        };
        let joins: Vec<WalkJoin> = (0..if cyclic { edges } else { edges - 1 })
            .map(|join| {
                let v = points[join];
                let w = points[wrap_index(join + 1, count)];
                let u = points[wrap_index(join + 2, count)];
                Some(match orientation(v, w, u) {
                    Orientation::CounterClockwise => match rule {
                        WalkJoinRule::Arc => {
                            let (nx, ny) = normals[join];
                            let (mx, my) = normals[wrap_index(join + 1, edges)];
                            if let Some(snap) = snapped(w, (nx, ny), (mx, my)) {
                                return Some(snap);
                            }
                            if nx * mx + ny * my >= cos_step {
                                return Some(WalkJoin::Straight);
                            }
                            let from_angle = ny.atan2(nx);
                            let mut sweep = my.atan2(mx) - from_angle;
                            if sweep < 0.0 {
                                sweep += std::f64::consts::TAU;
                            }
                            if sweep > std::f64::consts::PI {
                                return None;
                            }
                            WalkJoin::Arc { from_angle, sweep }
                        },
                        WalkJoinRule::Bevel => {
                            snapped(w, normals[join], normals[wrap_index(join + 1, edges)])
                                .unwrap_or(WalkJoin::Straight)
                        },
                        WalkJoinRule::Miter { limit } => convex_miter(
                            offset_edge(v, w, normals[join]),
                            offset_edge(w, u, normals[wrap_index(join + 1, edges)]),
                            w,
                            distance,
                            limit,
                        ),
                    },
                    Orientation::Clockwise => reflex_cross(
                        offset_edge(v, w, normals[join]),
                        offset_edge(w, u, normals[wrap_index(join + 1, edges)]),
                    )
                    .map_or_else(
                        || {
                            snapped(w, normals[join], normals[wrap_index(join + 1, edges)])
                                .unwrap_or(WalkJoin::Excursion)
                        },
                        |(point, t_in, t_out)| WalkJoin::Cross { point, t_in, t_out },
                    ),
                    Orientation::Collinear => WalkJoin::Straight,
                })
            })
            .collect::<Option<_>>()?;
        let mut plan = Self {
            points,
            normals,
            joins,
            cyclic,
            distance,
        };
        plan.validate();
        Some(plan)
    }

    /// Demote any inside crossing clipping an edge to a BACKWARDS
    /// traversal — entry parameter (the previous join's `t_out`) at or
    /// past the exit (its own join's `t_in`), the signature of an edge
    /// consumed by inversion. The through-vertex excursion is the
    /// always-correct fallback: its lobe cancels in the winding
    /// selection. Outside joins only EXTEND edges (their crossings sit
    /// past the endpoints), so they are never demotion candidates.
    /// Demotion only widens the neighbouring edges' parameter spans, so
    /// the fixpoint loop terminates (each round either demotes a
    /// crossing or stops).
    fn validate(&mut self) {
        let joins = self.joins.len();
        if joins == 0 {
            return;
        }
        let edges = self.normals.len();
        loop {
            let mut changed = false;
            for edge in 0..edges {
                let entry_join = if edge == 0 {
                    self.cyclic.then(|| joins - 1)
                } else {
                    Some(edge - 1)
                };
                let exit_join = (edge < joins).then_some(edge);
                let entry = entry_join.map_or(0.0, |join| match self.joins[join] {
                    WalkJoin::Cross { t_out, .. } => t_out,
                    _ => 0.0,
                });
                let exit = exit_join.map_or(1.0, |join| match self.joins[join] {
                    WalkJoin::Cross { t_in, .. } => t_in,
                    _ => 1.0,
                });
                if entry >= exit {
                    for join in [entry_join, exit_join].into_iter().flatten() {
                        if matches!(self.joins[join], WalkJoin::Cross { .. }) {
                            self.joins[join] = WalkJoin::Excursion;
                            changed = true;
                        }
                    }
                }
            }
            if !changed {
                break;
            }
        }
    }

    /// Append the walk's offset points: per-edge offsets stitched by the
    /// planned joins. Crossings and miters replace BOTH the incoming
    /// edge's end and the outgoing edge's start; on cyclic walks a wrap
    /// join of that kind simply suppresses the first edge's start point
    /// (rotation of a closed loop is free). Cyclic output is the open
    /// column form — the consumer closes the loop.
    pub(crate) fn emit(&self, step_angle: f64, xs: &mut Vec<f64>, ys: &mut Vec<f64>) {
        self.emit_impl::<false>(step_angle, xs, ys, &mut Vec::new());
    }

    /// [`WalkPlan::emit`] also recording each emitted point's SOURCE vertex
    /// (the walk index it derives from — join geometry, arcs, and excursion
    /// points all belong to their corner vertex). The offset-curve surface
    /// carries Z/M through this provenance; the buffer paths take the
    /// untracked monomorphization at zero cost.
    pub(crate) fn emit_tracked(
        &self,
        step_angle: f64,
        xs: &mut Vec<f64>,
        ys: &mut Vec<f64>,
        sources: &mut Vec<u32>,
    ) {
        self.emit_impl::<true>(step_angle, xs, ys, sources);
    }

    fn emit_impl<const TRACK: bool>(
        &self,
        step_angle: f64,
        xs: &mut Vec<f64>,
        ys: &mut Vec<f64>,
        sources: &mut Vec<u32>,
    ) {
        let count = self.points.len();
        // Exact adjacent duplicates (collinear runs, snapped joins meeting
        // edge offsets bit-for-bit) never reach the loop: every duplicate
        // here is a zero-length noding segment downstream.
        let push = |xs: &mut Vec<f64>, ys: &mut Vec<f64>, x: f64, y: f64| {
            if xs.last().is_some_and(|last| last.to_bits() == x.to_bits())
                && ys.last().is_some_and(|last| last.to_bits() == y.to_bits())
            {
                return;
            }
            xs.push(x);
            ys.push(y);
        };
        for (edge, &(nx, ny)) in self.normals.iter().enumerate() {
            let previous = if edge == 0 {
                self.cyclic.then(|| self.joins.len() - 1)
            } else {
                Some(edge - 1)
            };
            if !previous.is_some_and(|join| self.joins[join].replaces_endpoints()) {
                let v = self.points[edge];
                push(xs, ys, v.x + self.distance * nx, v.y + self.distance * ny);
            }
            if TRACK {
                sources.resize(xs.len(), edge as u32);
            }
            let w = self.points[wrap_index(edge + 1, count)];
            match self.joins.get(edge) {
                Some(
                    WalkJoin::Cross { point, .. }
                    | WalkJoin::Miter { point }
                    | WalkJoin::Snap { point },
                ) => {
                    push(xs, ys, point.x, point.y);
                },
                Some(WalkJoin::MiterCut { entry, exit }) => {
                    push(xs, ys, entry.x, entry.y);
                    push(xs, ys, exit.x, exit.y);
                },
                Some(WalkJoin::Arc { from_angle, sweep }) => {
                    push(xs, ys, w.x + self.distance * nx, w.y + self.distance * ny);
                    emit_arc(w, *from_angle, *sweep, self.distance, step_angle, xs, ys);
                },
                Some(WalkJoin::Excursion) => {
                    push(xs, ys, w.x + self.distance * nx, w.y + self.distance * ny);
                    push(xs, ys, w.x, w.y);
                },
                Some(WalkJoin::Straight) | None => {
                    push(xs, ys, w.x + self.distance * nx, w.y + self.distance * ny);
                },
            }
            if TRACK {
                sources.resize(xs.len(), (wrap_index(edge + 1, count)) as u32);
            }
        }
    }
}

/// Emit one cap at `tip` continuing from offset normal `(nx, ny)` (the
/// arriving side's normal at the tip): round sweeps pi; flat connects
/// directly (no points — the sides' endpoints already join); square adds
/// the two extended corners.
pub(crate) fn emit_cap(
    tip: Point,
    nx: f64,
    ny: f64,
    distance: f64,
    cap_style: BufferCapStyle,
    step_angle: f64,
    xs: &mut Vec<f64>,
    ys: &mut Vec<f64>,
) {
    match cap_style {
        BufferCapStyle::Round => {
            emit_arc(
                tip,
                ny.atan2(nx),
                std::f64::consts::PI,
                distance,
                step_angle,
                xs,
                ys,
            );
        },
        BufferCapStyle::Flat => {},
        BufferCapStyle::Square => {
            // The tip extends along the walk direction, the right-side
            // normal rotated +90 degrees: t = (-ny, nx).
            let (tx, ty) = (-ny, nx);
            xs.push(tip.x + distance * (nx + tx));
            ys.push(tip.y + distance * (ny + ty));
            xs.push(tip.x + distance * (-nx + tx));
            ys.push(tip.y + distance * (-ny + ty));
        },
    }
}

/// Clean one raw offset loop in its own arrangement and append its
/// surviving `winding <= -1` pieces to `loops` — oriented CW (each piece
/// arrives as a CCW shell from the polygonizer; `reverse` keeps the
/// subtracting orientation the final pass expects). Under erosion the
/// pieces ARE the shrunk-shell lobes and stay in the same CW form.
pub(crate) fn extend_cleaned(loops: &mut Vec<(Vec<f64>, Vec<f64>)>, raw: &(Vec<f64>, Vec<f64>)) {
    for piece in winding_region(std::slice::from_ref(raw), |winding| winding <= -1) {
        // Shrinking a simply-connected region cannot create nested
        // structure — each surviving piece is a bare shell.
        debug_assert!(piece.holes.is_empty());
        let mut xs: Vec<f64> = piece.shell.coords().xs().to_vec();
        let mut ys: Vec<f64> = piece.shell.coords().ys().to_vec();
        // Drop the closing duplicate and orient CW.
        xs.pop();
        ys.pop();
        xs.reverse();
        ys.reverse();
        loops.push((xs, ys));
    }
}

/// Faces of the noded arrangement of the directed column `loops` whose
/// winding number passes `keep`, re-assembled into nested polygons — the
/// shared region engine behind both of the buffer's winding selections,
/// running on the columnar half-edge [`Arrangement`] core: directed
/// multiplicities give every face's winding from ONE probe per connected
/// component (components never cross, so the winding of all OTHER loops is
/// constant across each), propagated by BFS over twin half-edges.
pub(in crate::geometry) fn winding_region(
    loops: &[(Vec<f64>, Vec<f64>)],
    keep: impl Fn(i32) -> bool,
) -> Vec<Polygon> {
    let mut segments = Vec::new();
    let mut segment_loop: Vec<u32> = Vec::new();
    let mut loop_anchor: Vec<XY> = Vec::new();
    let mut loop_ranges: Vec<(u32, u32)> = Vec::new();
    for (xs, ys) in loops {
        let before = segments.len();
        for index in 0..xs.len() {
            let next = wrap_index(index + 1, xs.len());
            let start = Point::new_unchecked_xy(xs[index], ys[index]);
            let end = Point::new_unchecked_xy(xs[next], ys[next]);
            if !same_point(start, end) {
                segments.push(Segment {
                    start: start.xy(),
                    end: end.xy(),
                });
                segment_loop.push(loop_anchor.len() as u32);
            }
        }
        if segments.len() > before {
            loop_ranges.push((before as u32, segments.len() as u32));
            loop_anchor.push(XY::new(xs[0], ys[0]));
        }
    }
    // Single-loop fast construction (the stroke buffer's whole caseload,
    // single-ring expansions/erosions too): one closed chain's topology is
    // positional, so the vertex hashing, the atomic segment soup, and the
    // edge sort all collapse into one ordinal walk. The arrangement is
    // bit-identical to the general build below, which stays the exact
    // oracle and owns every bail (overlaps, T-junctions, repeats).
    if loop_anchor.len() == 1
        && let Some(arrangement) = Arrangement::from_single_loop(&segments)
    {
        let windings = arrangement.face_windings(&[0]);
        return assemble_region_polygons(arrangement.region_rings(&windings, keep));
    }
    // MULTI-loop positional construction (dissolve leaves, multipolygon
    // structure repair): the same positional win across K clean loops,
    // with the general build as the bail oracle. Seeds resolve through
    // the shared outside-winding logic below either way.
    let arrangement = Arrangement::<i32>::from_loops(&segments, &loop_ranges, |_| 1)
        .unwrap_or_else(|| {
            Arrangement::new(&crate::geometry::overlay::self_node_segments(&segments))
        });
    let probes = arrangement.component_probes();
    // Single-loop fast seed: one closed loop nodes into ONE connected
    // component (noding never disconnects a curve), and the unbounded
    // face's winding is 0 by definition — no per-loop ray queries, no
    // outside-winding resolution.
    let seeds: Vec<i32> = if loop_anchor.len() == 1 && probes.len() == 1 {
        vec![0]
    } else {
        // Outside-winding seed per component: every loop lives in exactly
        // one component, and loops of other components cannot pass through
        // it — one ray query each (see `outside_winding_seeds`).
        let loop_component: Vec<u32> = loop_anchor
            .iter()
            .map(|&anchor| arrangement.component_of_point(anchor))
            .collect();
        let segment_operand = vec![0_u32; segments.len()];
        crate::geometry::overlay::outside_winding_seeds(
            &segments,
            &segment_operand,
            &segment_loop,
            &loop_component,
            &probes,
        )
        .into_iter()
        .map(|seed| seed[0])
        .collect()
    };
    let windings = arrangement.face_windings(&seeds);
    // Direct boundary-ring walk: shells arrive CCW and holes CW with no
    // re-polygonization.
    assemble_region_polygons(arrangement.region_rings(&windings, keep))
}

/// Nest boundary rings (shells CCW, holes CW — the
/// [`Arrangement::region_rings`] convention) into polygons: each hole goes
/// to the SMALLEST containing shell.
pub(in crate::geometry) fn assemble_region_polygons(rings: Vec<Vec<XY>>) -> Vec<Polygon> {
    let mut shells: Vec<(Vec<XY>, f64, Bounds)> = Vec::new();
    let mut holes: Vec<Vec<XY>> = Vec::new();
    for ring in rings {
        let decision = open_xy_cycle_decision(&ring);
        let (sign, magnitude) = (decision.sign(), decision.magnitude());
        match sign {
            AreaSign::Positive => {
                let bounds = Bounds::from_xy_iter(ring.iter().copied());
                shells.push((ring, magnitude.get(), bounds));
            },
            AreaSign::Negative => holes.push(ring),
            AreaSign::Zero => {},
        }
    }
    shells.sort_by(|left, right| left.1.total_cmp(&right.1));
    let mut assigned: Vec<Vec<Ring>> = vec![Vec::new(); shells.len()];
    for hole in holes {
        // A pinch vertex (a hole touching its shell at a point) lies ON
        // the shell and fails the strict-interior test; any other vertex
        // of the hole decides ownership, so probe until one answers.
        let owner = shells.iter().position(|(shell, _, bounds)| {
            hole.iter()
                .any(|probe| bounds.contains_xy(*probe) && ring_contains_interior(shell, *probe))
        });
        if let Some(owner) = owner {
            assigned[owner].push(Ring::from_trusted_closed(hole));
        }
    }
    shells
        .into_iter()
        .zip(assigned)
        .map(|((shell, ..), holes)| Polygon::new(Ring::from_trusted_closed(shell), holes))
        .collect()
}
