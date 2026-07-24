#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;
use crate::error::Result;
use crate::geometry::*;
impl Shape {
    pub(crate) fn intersection(&self, other: &Self, strictness: Strictness) -> Result<Self> {
        self.overlay(other, OverlayOp::Intersection, strictness)
    }

    pub(crate) fn union(&self, other: &Self, strictness: Strictness) -> Result<Self> {
        self.overlay(other, OverlayOp::Union, strictness)
    }

    pub(crate) fn difference(&self, other: &Self, strictness: Strictness) -> Result<Self> {
        self.overlay(other, OverlayOp::Difference, strictness)
    }

    pub(crate) fn symmetric_difference(
        &self,
        other: &Self,
        strictness: Strictness,
    ) -> Result<Self> {
        self.overlay(other, OverlayOp::SymmetricDifference, strictness)
    }

    /// Bounds-disjoint short-circuit (GEOS's envelope optimization with
    /// gometry's consistent narrowing): operands whose bounds do not
    /// intersect never node — union / symmetric difference is both side
    /// by side, difference is the left operand, intersection is empty.
    /// Input chains and structure are preserved verbatim; `None` when the
    /// bounds DO overlap (the caller falls through to containment/pipeline).
    pub(crate) fn disjoint_shortcut(
        &self,
        other: &Self,
        op: OverlayOp,
        left: Option<Bounds>,
        right: Option<Bounds>,
    ) -> Option<Self> {
        let (left, right) = (left?, right?);
        if left.intersects(right) {
            return None;
        }
        match op {
            // Bounds-disjoint intersection is empty — return the dimension-typed
            // empty straight away instead of routing an empty part set through
            // the survivor machinery.
            OverlayOp::Intersection => Some(empty_overlay_shape(self, other, op)),
            OverlayOp::Difference => {
                let mut parts = DimensionalParts::default();
                parts.push_shape(self);
                Some(build_overlay_shape(
                    parts.points,
                    parts.lines.iter().map(|line| (*line).clone()).collect(),
                    parts
                        .polygons
                        .iter()
                        .map(|polygon| (*polygon).clone())
                        .collect(),
                ))
            },
            OverlayOp::Union | OverlayOp::SymmetricDifference => {
                Some(disjoint_overlay_combine(self, other))
            },
        }
    }

    /// CONTAINED short-circuit (the disjoint gate's sibling): an areal
    /// operand STRICTLY inside the other's interior (`contains_properly` —
    /// no linework contact anywhere) resolves every op without noding; a
    /// difference just punches the subtrahend in as a hole. The
    /// strict-bounds pre-filter keeps the prepared probe off ordinary
    /// overlapping pairs, and the size gate (mirroring the window clip)
    /// keeps small pairs on the plain engine. Ring reuse carries Z/M by
    /// construction.
    pub(crate) fn contained_shortcut(
        &self,
        other: &Self,
        op: OverlayOp,
        self_bounds: Option<Bounds>,
        other_bounds: Option<Bounds>,
    ) -> Option<Self> {
        contained_shortcut_impl(
            self,
            other,
            op,
            self_bounds,
            other_bounds,
            |left_is_outer| {
                if left_is_outer {
                    self.contains_properly(other)
                } else {
                    other.contains_properly(self)
                }
            },
        )
    }

    /// Dispatch an overlay op, then normalize an empty result to the op's
    /// dimension-typed empty (see [`empty_overlay_shape`]). A vanishing
    /// intersection/difference/symdiff/union participates in gometry's
    /// typed-empty model — `POLYGON EMPTY` for a polygon−polygon difference
    /// that cancels, not an untyped `GEOMETRYCOLLECTION EMPTY` stray among
    /// typed siblings in an array result.
    pub(crate) fn overlay(
        &self,
        other: &Self,
        op: OverlayOp,
        strictness: Strictness,
    ) -> Result<Self> {
        let result = self.overlay_inner(other, op, strictness)?;
        Ok(if result.is_empty() {
            empty_overlay_shape(self, other, op)
        } else {
            result
        })
    }

    /// Mixed-dimension set overlay over X/Y, restoring Z/M afterwards via
    /// `carry_ordinates` (or dropping it when `strict`).
    ///
    /// Decomposes both operands into point/line/polygon buckets, evaluates the
    /// operation per dimension, absorbs lower-dimensional pieces covered by the
    /// polygon output, and narrows to the tightest representable `Shape`.
    pub(crate) fn overlay_inner(
        &self,
        other: &Self,
        op: OverlayOp,
        strictness: Strictness,
    ) -> Result<Self> {
        // Two no-noding shortcuts first — bounds-disjoint and proper
        // containment both resolve every op by re-presenting input parts
        // (see each helper); only genuinely interacting operands reach the
        // mixed-dimension pipeline below. Both gates need the operand bounds, so
        // scan each ONCE here and share them (the disjoint and contained checks
        // would otherwise each rescan every coordinate — 4× the work).
        let (self_bounds, other_bounds) = (self.bounds(), other.bounds());
        if let Some(shape) = self.disjoint_shortcut(other, op, self_bounds, other_bounds) {
            return carry_ordinates(shape, &[self, other], op.name(), strictness.is_strict());
        }
        if let Some(shape) = self.contained_shortcut(other, op, self_bounds, other_bounds) {
            // A containment shortcut re-presents an input operand verbatim, so
            // every vertex is faithful — Z/M is correct as-is under both
            // policies (nothing synthesized to source or reject).
            return Ok(shape);
        }
        // Clean-case exact overlay fast path: two simple 2D polygons (shells AND
        // holes) meeting only at proper transverse crossings reassemble directly from
        // their result-boundary arcs (endpoint-chained, single or multi-shell)
        // — skipping the DCEL/face-BFS. Wired for union, difference,
        // symmetric_difference, and intersection. Symmetric_difference cannot
        // share the single-pass walk (its a−b and b−a pieces meet at every
        // crossing → a pinch), so `clean_overlay` reassembles the two clean
        // difference arc sets separately and assembles the combined rings.
        // Intersection defers to the rectangle clip when a rectangle operand
        // makes that the faster specialized path. Bails to the exact arrangement
        // on any degeneracy (the arrangement stays the oracle; a per-op fuzz pins
        // equality). XY-only, so Z/M defers.
        let clean_op = matches!(
            op,
            OverlayOp::Union | OverlayOp::Difference | OverlayOp::SymmetricDifference
        ) || (op == OverlayOp::Intersection
            && axis_rectangle(self).is_none()
            && axis_rectangle(other).is_none());
        // Polygonal operand as a component slice: a `Polygon` is one component,
        // a `MultiPolygon` its parts. The clean fast path handles both (membership
        // is even-odd across all the other operand's rings).
        let left_parts: Option<&[Polygon]> = match self {
            Self::Polygon(polygon) => Some(std::slice::from_ref(polygon)),
            Self::MultiPolygon(polygons) => Some(polygons.as_slice()),
            _ => None,
        };
        let right_parts: Option<&[Polygon]> = match other {
            Self::Polygon(polygon) => Some(std::slice::from_ref(polygon)),
            Self::MultiPolygon(polygons) => Some(polygons.as_slice()),
            _ => None,
        };
        if clean_op
            && !self.has_z()
            && !self.has_m()
            && !other.has_z()
            && !other.has_m()
            && let (Some(left), Some(right)) = (left_parts, right_parts)
            && let Some(shape) = crate::geometry::clean_union::clean_overlay(left, right, op)
        {
            return carry_ordinates(shape, &[self, other], op.name(), strictness.is_strict());
        }
        // Rectangle-operand intersection (GEOS's RectangleIntersection
        // routing at clip parity): the areal bucket is the linear clip,
        // the SUBJECT being the non-rectangle operand. Resolved ONCE here
        // — which side is the rectangle and its bounds — and threaded
        // through region assembly + boundary contact; window clipping is
        // skipped (it would only duplicate the clip). The boundary,
        // line, and point machinery below stays untouched, so
        // dimension-collapse and Z/M semantics are exact.
        let rect_routing = (op == OverlayOp::Intersection)
            .then(|| {
                axis_rectangle(self)
                    .map(|rect| (rect, RectSide::Left))
                    .or_else(|| axis_rectangle(other).map(|rect| (rect, RectSide::Right)))
            })
            .flatten();
        let (clipped_self, clipped_other) = if rect_routing.is_some() {
            (None, None)
        } else {
            window_clip_large_operands(self, other, op)
        };
        let left_shape = clipped_self.as_ref().unwrap_or(self);
        let right_shape = clipped_other.as_ref().unwrap_or(other);
        let left = DimensionalParts::from_shape(left_shape);
        let right = DimensionalParts::from_shape(right_shape);
        // The rectangle's subject is the OTHER operand's polygons.
        let rect_subject = |routing: &(Bounds, RectSide)| match routing.1 {
            RectSide::Left => &right.polygons,
            RectSide::Right => &left.polygons,
        };
        // One joint arrangement serves BOTH the region assembly and the
        // boundary-contact verdicts (the rect lane keeps its own scan).
        let mut built_arrangement = None;
        let result_polygons = if let Some(routing) = &rect_routing {
            clip_polygonal_parts(rect_subject(routing), routing.0)
        } else if left.polygons.is_empty() || right.polygons.is_empty() {
            binary_areal_overlay(&left.polygons, &right.polygons, op)
        } else {
            let built = build_areal_arrangement(&left.polygons, &right.polygons);
            let polygons = built.overlay_polygons(op);
            built_arrangement = Some(built);
            polygons
        };
        // Polygon∩polygon boundary contact (GEOS dimension semantics): one
        // envelope-indexed pass finds the isolated touch points and whether
        // any positive-length boundary run is shared — the trigger for the
        // shared-edge noding in `overlay_lines_general`.
        let mut boundary_touch_points = Vec::new();
        let mut shares_boundary = false;
        if op == OverlayOp::Intersection && !left.polygons.is_empty() && !right.polygons.is_empty()
        {
            (boundary_touch_points, shares_boundary) = match (&rect_routing, &built_arrangement) {
                // Contact with a rectangle lives ON its boundary — the
                // columnar prefilter drops everything strictly inside or
                // fully outside before the exact kernels run.
                (Some(routing), _) => rect_boundary_contact(rect_subject(routing), routing.0),
                (None, Some(built)) => built.boundary_contact(),
                (None, None) => (Vec::new(), false),
            };
        }
        let lines = overlay_lines_general(&left, &right, &result_polygons, op, shares_boundary);
        let mut points = overlay_points(left_shape, right_shape, &left, &right, op);
        if op == OverlayOp::Intersection {
            // Isolated 0-D contacts: line×line crossings, line/polygon-boundary
            // grazes, and polygon-boundary corner touches. `build_overlay_shape`
            // absorbs any that lie on a retained line or inside the result
            // polygon.
            points.extend(line_line_cross_points(
                &collect_line_segments(&left.lines),
                &collect_line_segments(&right.lines),
            ));
            points.extend(polygon_line_touch_points(&left, &right));
            points.extend(boundary_touch_points.into_iter().map(XY::point));
        }
        let shape = build_overlay_shape(points, lines, result_polygons);
        carry_ordinates(shape, &[self, other], op.name(), strictness.is_strict())
    }

    /// Dissolve every geometry into one mixed-dimension union.
    ///
    /// Polygons are dissolved in a single `unary_union`; linework is noded and
    /// clipped to the polygon-free region; points survive only where no higher
    /// dimension covers them. Z/M is restored afterwards unless `strict`.
    pub(crate) fn union_all<S: std::borrow::Borrow<Self>>(
        geometries: &[S],
        strictness: Strictness,
    ) -> Result<Self> {
        if geometries.is_empty() {
            return Err(GeometryErrorKind::EmptyGeometrySequence {
                operation: "union_all",
            }
            .into());
        }
        let mut parts = DimensionalParts::default();
        for geometry in geometries {
            parts.push_shape(geometry.borrow());
        }
        let dissolved = dissolve_polygons(
            parts
                .polygons
                .iter()
                .map(|polygon| (*polygon).clone())
                .collect(),
        );
        let lines = union_lines(&parts.lines, &dissolved);
        let shape = build_overlay_shape(parts.points.clone(), lines, dissolved);
        carry_ordinates(
            shape,
            &geometries
                .iter()
                .map(std::borrow::Borrow::borrow)
                .collect::<Vec<_>>(),
            "union_all",
            strictness.is_strict(),
        )
    }

    /// Reduce a sequence to its common intersection — `g0 ∩ g1 ∩ … ∩ gn`, the
    /// region inside EVERY input. Mirrors `union_all`'s sequence contract
    /// (raises on an empty sequence; one geometry returns itself). The fold
    /// rides the same pairwise [`Self::intersection`], so mixed-dimension
    /// narrowing, CRS handling, and the `strict`/typed-empty rules are
    /// identical; once the running result empties, the remaining steps
    /// short-circuit.
    pub(crate) fn intersection_all<S: std::borrow::Borrow<Self>>(
        geometries: &[S],
        strictness: Strictness,
    ) -> Result<Self> {
        Self::intersection_all_ordered(
            &geometries
                .iter()
                .map(std::borrow::Borrow::borrow)
                .collect::<Vec<_>>(),
            strictness,
        )
    }

    /// Reduce a sequence by symmetric difference — `g0 ▵ g1 ▵ … ▵ gn`, the
    /// region covered by an ODD number of inputs. Same-dimension inputs use a
    /// balanced, order-independent cascade; mixed-dimension inputs keep the
    /// input-order fold because binary symdiff is not associative across
    /// dimensions. Same sequence contract and pairwise machinery as
    /// [`Self::intersection_all`].
    pub(crate) fn symmetric_difference_all<S: std::borrow::Borrow<Self>>(
        geometries: &[S],
        strictness: Strictness,
    ) -> Result<Self> {
        Self::symmetric_difference_all_balanced(
            &geometries
                .iter()
                .map(std::borrow::Borrow::borrow)
                .collect::<Vec<_>>(),
            strictness,
        )
    }

    /// Reduce intersections by combining the most selective intermediates
    /// first. Internal reductions are XY-only. Z/M is carried once from the
    /// ORIGINAL inputs (coverage-based, order-independent — consistent with
    /// `union_all`), NOT from pairwise intermediates.
    pub(crate) fn intersection_all_ordered(
        inputs: &[&Self],
        strictness: Strictness,
    ) -> Result<Self> {
        if inputs.is_empty() {
            return Err(GeometryErrorKind::EmptyGeometrySequence {
                operation: "intersection_all",
            }
            .into());
        }
        if let [input] = inputs {
            // A single operand intersects to itself — faithful, so Z/M is kept.
            return Ok((*input).clone());
        }
        let Some(mut window) = inputs[0].bounds() else {
            return Ok(empty_nary_overlay_shape(inputs, OverlayOp::Intersection));
        };
        for input in &inputs[1..] {
            let Some(bounds) = input.bounds() else {
                return Ok(empty_nary_overlay_shape(inputs, OverlayOp::Intersection));
            };
            window = Bounds::new_unchecked(
                window.minx().max(bounds.minx()),
                window.miny().max(bounds.miny()),
                window.maxx().min(bounds.maxx()),
                window.maxy().min(bounds.maxy()),
            );
            if window.minx() > window.maxx() || window.miny() > window.maxy() {
                return Ok(empty_nary_overlay_shape(inputs, OverlayOp::Intersection));
            }
        }

        let mut serial = inputs.len();
        let mut heap: std::collections::BinaryHeap<OverlayWorkItem> = inputs
            .iter()
            .enumerate()
            .map(|(index, input)| OverlayWorkItem::new((*input).clone(), index))
            .collect();
        while heap.len() > 1 {
            let left = heap.pop().expect("left work item").shape;
            let right = heap.pop().expect("right work item").shape;
            // Intermediate reductions stay XY-only; Z/M is carried once at the
            // end from the ORIGINAL inputs (so no strict-raise on intermediates).
            let reduced = left.overlay(&right, OverlayOp::Intersection, Strictness::Lenient)?;
            if reduced.is_empty() {
                return Ok(empty_nary_overlay_shape(inputs, OverlayOp::Intersection));
            }
            heap.push(OverlayWorkItem::new(reduced, serial));
            serial += 1;
        }
        let reduced = heap.pop().expect("final work item").shape;
        carry_ordinates(reduced, inputs, "intersection_all", strictness.is_strict())
    }

    /// Reduce symmetric difference by bbox clusters. Same-dimension clusters
    /// take a balanced binary cascade; mixed-dimension clusters keep input
    /// order because pairwise symdiff is not associative across dimensions.
    /// Internal reductions are XY-only.
    /// Z/M is carried once from the ORIGINAL inputs (coverage-based,
    /// order-independent — consistent with `union_all`), NOT from pairwise
    /// intermediates.
    pub(crate) fn symmetric_difference_all_balanced(
        inputs: &[&Self],
        strictness: Strictness,
    ) -> Result<Self> {
        if inputs.is_empty() {
            return Err(GeometryErrorKind::EmptyGeometrySequence {
                operation: "symmetric_difference_all",
            }
            .into());
        }
        let mixed_dimensions = has_mixed_non_empty_topological_dimensions(inputs);
        let work: Vec<_> = inputs
            .iter()
            .enumerate()
            .filter(|(_, input)| !input.is_empty())
            .map(|(index, input)| OverlayWorkItem::new((*input).clone(), index))
            .collect();
        if work.is_empty() {
            return Ok(empty_nary_overlay_shape(
                inputs,
                OverlayOp::SymmetricDifference,
            ));
        }

        let clusters = overlay_work_clusters(work);
        let mut cluster_results = Vec::with_capacity(clusters.len());
        for cluster in clusters {
            let reduced = if mixed_dimensions {
                symmetric_difference_cluster_ordered(cluster)?
            } else {
                symmetric_difference_cluster_balanced(cluster)?
            };
            if !reduced.is_empty() {
                cluster_results.push(reduced);
            }
        }
        let combined = match cluster_results.len() {
            0 => empty_nary_overlay_shape(inputs, OverlayOp::SymmetricDifference),
            1 => cluster_results.pop().expect("one cluster result"),
            _ => Self::union_all(&cluster_results, Strictness::Lenient)?,
        };
        carry_ordinates(
            combined,
            inputs,
            "symmetric_difference_all",
            strictness.is_strict(),
        )
    }

    /// Node this geometry's linework: split every edge at all intersections
    /// (crossings and shared points) and return the noded edges as a
    /// `MultiLineString` (GEOS/PostGIS `ST_Node`). Polygon boundaries are
    /// included; point-only input yields `MULTILINESTRING EMPTY`. Z/M is
    pub(crate) fn node(&self, strictness: Strictness) -> Result<Self> {
        let noded = dedup_segments_to_coordseqs(self_node_segments(&self.segments()))
            .into_iter()
            .map(|line| LineSeq::try_new(line).expect("noded segment has two vertices"))
            .collect();
        carry_ordinates(
            Self::MultiLineString(noded),
            &[self],
            "node",
            strictness.is_strict(),
        )
    }

    pub fn line_merge(&self) -> Result<Self> {
        match self {
            Self::LineString(_) => Ok(self.clone()),
            Self::MultiLineString(lines) => {
                // Dissolve the component lines into maximal chains at degree-2
                // nodes, splitting every Y/T/X junction (degree `>= 3`) — the
                // JTS LineMerger contract, shared verbatim with the overlay line
                // sink via [`merge_chains`].
                let mut chains =
                    merge_chains(lines.iter().map(|line| line.as_coords().clone()).collect());
                Ok(match chains.len() {
                    0 => Self::MultiLineString(Vec::new()),
                    1 => Self::LineString(
                        LineSeq::try_new(chains.pop().expect("len == 1"))
                            .expect("merged chain is lineal"),
                    ),
                    _ => Self::MultiLineString(
                        chains
                            .into_iter()
                            .map(|line| LineSeq::try_new(line).expect("merged chain is lineal"))
                            .collect(),
                    ),
                })
            },
            _ => Err(GeometryErrorKind::LinealRequired.into()),
        }
    }

    pub fn shared_paths(&self, other: &Self) -> Result<Self> {
        let left = self
            .linework()
            .map_err(|_| GeometryErrorKind::LinealRequired)?;
        let right = other
            .linework()
            .map_err(|_| GeometryErrorKind::LinealRequired)?;
        let mut same = Vec::new();
        let mut opposite = Vec::new();
        let mut same_seen: HashSet<(PointKey, PointKey)> = HashSet::new();
        let mut opposite_seen: HashSet<(PointKey, PointKey)> = HashSet::new();
        let push_shared = |lines: &mut Vec<Vec<Point>>,
                           seen: &mut HashSet<(PointKey, PointKey)>,
                           line: Vec<Point>| {
            if line.len() >= 2 {
                let key = undirected_segment_edge_key(Segment {
                    start: line[0].xy(),
                    end: line[line.len() - 1].xy(),
                });
                if seen.insert(key) {
                    lines.push(line);
                }
            }
        };
        // Flattened segment lists keep the line-major iteration order; the
        // candidate engine prunes disjoint-envelope pairs above the
        // crossover with the same observable order.
        let left_segments: Vec<Segment> =
            left.iter().flat_map(|line| line_segments(*line)).collect();
        let right_segments: Vec<Segment> =
            right.iter().flat_map(|line| line_segments(*line)).collect();
        for_each_overlapping_pair::<true>(
            &left_segments,
            &right_segments,
            |left_segment, right_segment| {
                if let Some((same_direction, shared)) =
                    shared_segment_part(left_segment, right_segment)
                {
                    let shared: Vec<Point> = shared.into_iter().map(XY::point).collect();
                    if same_direction {
                        push_shared(&mut same, &mut same_seen, shared);
                    } else {
                        push_shared(&mut opposite, &mut opposite_seen, shared);
                    }
                }
            },
        );
        let opposite_keys: HashSet<(PointKey, PointKey)> = opposite_seen;
        same.retain(|line| {
            line.len() < 2
                || !opposite_keys.contains(&undirected_segment_edge_key(Segment {
                    start: line[0].xy(),
                    end: line[line.len() - 1].xy(),
                }))
        });
        Ok(Self::GeometryCollection(vec![
            Self::MultiLineString(
                same.into_iter()
                    .map(CoordSeq::from)
                    .map(|line| LineSeq::try_new(line).expect("shared path is lineal"))
                    .collect(),
            ),
            Self::MultiLineString(
                opposite
                    .into_iter()
                    .map(CoordSeq::from)
                    .map(|line| LineSeq::try_new(line).expect("shared path is lineal"))
                    .collect(),
            ),
        ]))
    }
}
