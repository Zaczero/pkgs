use crate::error::Result;
use crate::geometry::predicates::{
    bounds_cover, bounds_equal_topological, convex_covers_all_vertices, disjoint_de9im,
    exterior_part_uniform, has_collection_operand, interior_part_uniform, interiors_meet_uniform,
    intersection_contact, isolated_point_contact, line_contains_point, multiline_contains_point,
    option_bounds_disjoint, point_is_geographic_pole, pole_position, strict_interior_witness,
    vertex_witness,
};
use crate::geometry::{
    Bounds, Coordinates as _, DimMode, Dimension, EmptyKind, GeometryErrorKind, Point,
    SegmentContact, Shape, dimension, lineal_relate_shapes, linework_contact, mixed_relate_shapes,
    native_relate_pattern_shapes, native_relate_shapes, point_equals_exact, point_on_segment,
    points_equal_exact, relate_ng, same_point, topology_split,
};

impl Shape {
    /// Strict OGC ``contains`` for a point candidate: boundary points are
    /// excluded (a line's mod-2 endpoints, a polygon's rings). This is the
    /// single point kernel behind the scalar predicate, the vectorized fast
    /// path, ``contains_xy``, and coverage membership — they cannot diverge.
    pub fn contains_point(&self, point: Point) -> bool {
        self.point_membership::<false>(point)
    }

    pub fn covers_point(&self, point: Point) -> bool {
        self.point_membership::<true>(point)
    }

    /// One point-membership skeleton for both spellings: `BOUNDARY`
    /// selects boundary-inclusive `covers` semantics over interior-only
    /// `contains` in the three arms where they differ — the const flag
    /// keeps a single exhaustive match, so a new `Shape` variant cannot
    /// update one predicate and silently miss its twin.
    fn point_membership<const BOUNDARY: bool>(&self, point: Point) -> bool {
        match self {
            Self::Point(value) => same_point(*value, point),
            Self::MultiPoint(points) => points.iter().any(|value| same_point(value, point)),
            Self::LineString(points) => {
                if BOUNDARY {
                    points
                        .segment_pairs()
                        .any(|[start, end]| point_on_segment(point, start, end))
                } else {
                    line_contains_point(points, point)
                }
            },
            Self::MultiLineString(lines) => {
                if BOUNDARY {
                    lines.iter().any(|line| {
                        line.segment_pairs()
                            .any(|[start, end]| point_on_segment(point, start, end))
                    })
                } else {
                    multiline_contains_point(lines, point)
                }
            },
            Self::Polygon(polygon) => {
                if BOUNDARY {
                    polygon.covers_point(point)
                } else {
                    polygon.contains_point(point)
                }
            },
            Self::MultiPolygon(polygons) => polygons.iter().any(|polygon| {
                if BOUNDARY {
                    polygon.covers_point(point)
                } else {
                    polygon.contains_point(point)
                }
            }),
            Self::GeometryCollection(_) => {
                let matrix = native_relate_shapes(self, &Self::Point(point.to_xy()));
                if BOUNDARY {
                    matrix.is_covers()
                } else {
                    matrix.is_contains()
                }
            },
            Self::Empty(..) => false,
        }
    }

    pub fn contains(&self, other: &Self) -> bool {
        self.contains_with_bounds::<true>(other, self.bounds(), other.bounds(), || {
            linework_contact(self, other)
        })
    }

    /// [`contains`](Self::contains) with caller-supplied bounds — the
    /// prepared-handle lanes pass their CACHED boxes (see
    /// `ShapeData::contains_cached`).
    /// `WITNESS = false` callers MUST have already run the
    /// uncovered-vertex refutation themselves (the cached shadows probe
    /// through the handle's hierarchical `PointBatchTester` — same budget,
    /// cheaper membership probes).
    pub(crate) fn contains_with_bounds<const WITNESS: bool>(
        &self,
        other: &Self,
        own: Option<Bounds>,
        theirs: Option<Bounds>,
        contact: impl FnOnce() -> SegmentContact,
    ) -> bool {
        if has_collection_operand(self, other) {
            return native_relate_shapes(self, other).is_contains();
        }
        // Point candidates use the strict point kernel: it implements the
        // OGC mod-2 boundary rule for multilines (matching `relate` and
        // Shapely), which the geo backend's contains does not.
        if let Self::Point(point) = other {
            return self.contains_point(*point);
        }
        // Containment of bounds is NECESSARY (a candidate poking out of
        // the container's box is never contained), and a candidate vertex
        // strictly outside the container settles near-misses — both
        // answer before the intersection matrix.
        match (own, theirs) {
            (Some(outer), Some(inner)) if !bounds_cover(outer, inner) => return false,
            (None, _) | (_, None) => return false,
            _ => {},
        }
        if WITNESS && vertex_witness(other, |point| !self.covers_point(point)) {
            return false;
        }
        // Convex hole-free container with every candidate vertex covered
        // (the witness just scanned them all): the candidate's hull —
        // hence the whole candidate — lies within the container, and a
        // positive-AREA candidate cannot hide inside the boundary curve,
        // so containment is settled without any scan (the index-refine
        // hot case: box queries over small candidates).
        if other.has_area_parts() && convex_covers_all_vertices(self, other) {
            return true;
        }
        if self.topology_dimension() == Some(Dimension::Surface) {
            match contact() {
                // The candidate's boundary properly crossing the
                // container's puts candidate interior strictly outside
                // (see `linework_contact`).
                SegmentContact::Cross => return false,
                // No boundary contact: containment is exactly `interiors
                // meet, and no candidate part in the exterior` — decided
                // by representatives.
                SegmentContact::None => {
                    return interior_part_uniform(other, self)
                        && !exterior_part_uniform(other, self);
                },
                SegmentContact::Touch => {},
            }
        }
        native_relate_shapes(self, other).is_contains()
    }

    /// `contains` with no boundary contact: *other* lies in the interior of
    /// *self* and never touches its boundary (DE-9IM `T**FF*FF*`).
    pub fn contains_properly(&self, other: &Self) -> bool {
        self.contains_properly_with_bounds(other, self.bounds(), other.bounds(), || {
            linework_contact(self, other)
        })
    }

    pub(crate) fn contains_properly_with_bounds(
        &self,
        other: &Self,
        own: Option<Bounds>,
        theirs: Option<Bounds>,
        contact: impl FnOnce() -> SegmentContact,
    ) -> bool {
        if has_collection_operand(self, other) {
            return native_relate_shapes(self, other).is_contains_properly();
        }
        // For a point candidate this coincides with strict containment (a
        // point in the interior cannot touch the boundary), so reuse the
        // mod-2-correct point kernel.
        if let Self::Point(point) = other {
            return self.contains_point(*point);
        }
        // Proper containment implies containment, so the `contains` gates
        // refute it the same way: bounds containment is NECESSARY, a
        // candidate vertex outside the container settles near-misses, and
        // empty operands (`None` bounds) never participate — the relate
        // graph is not built for them.
        match (own, theirs) {
            (Some(outer), Some(inner)) if !bounds_cover(outer, inner) => return false,
            (None, _) | (_, None) => return false,
            _ => {},
        }
        if vertex_witness(other, |point| !self.covers_point(point)) {
            return false;
        }
        if self.topology_dimension() == Some(Dimension::Surface) {
            match contact() {
                SegmentContact::Cross => return false,
                // No boundary contact: proper containment is exactly
                // `every candidate part strictly inside` — a bare point
                // part resting ON the container's boundary (the one
                // contact the linework scan cannot see) fails the strict
                // kernel, and a nonempty candidate fully interior makes
                // the interiors meet.
                SegmentContact::None => {
                    return !other
                        .any_component_representative(&mut |point| !self.contains_point(point));
                },
                SegmentContact::Touch => {},
            }
        }
        native_relate_shapes(self, other).is_contains_properly()
    }

    pub fn within(&self, other: &Self) -> bool {
        other.contains(self)
    }

    pub fn covers(&self, other: &Self) -> bool {
        self.covers_with_bounds::<true>(other, self.bounds(), other.bounds(), || {
            linework_contact(self, other)
        })
    }

    /// [`covers`](Self::covers) with caller-supplied bounds (see
    /// [`contains_with_bounds`](Self::contains_with_bounds)).
    /// See [`contains_with_bounds`](Self::contains_with_bounds) for the
    /// `WITNESS` contract.
    pub(crate) fn covers_with_bounds<const WITNESS: bool>(
        &self,
        other: &Self,
        own: Option<Bounds>,
        theirs: Option<Bounds>,
        contact: impl FnOnce() -> SegmentContact,
    ) -> bool {
        if has_collection_operand(self, other) {
            return native_relate_shapes(self, other).is_covers();
        }
        // Point candidates ARE the point kernel (covers is the
        // boundary-inclusive membership).
        if let Self::Point(point) = other {
            return self.covers_point(*point);
        }
        // The same necessary conditions as `contains` (covers only relaxes
        // the boundary-contact rule, which neither gate depends on).
        match (own, theirs) {
            (Some(outer), Some(inner)) if !bounds_cover(outer, inner) => return false,
            (None, _) | (_, None) => return false,
            _ => {},
        }
        if WITNESS && vertex_witness(other, |point| !self.covers_point(point)) {
            return false;
        }
        // Convex hole-free container + every candidate vertex covered ⇒
        // the candidate's hull lies within the container — covers needs
        // no interior-meet, so ANY candidate dimension settles here.
        if convex_covers_all_vertices(self, other) {
            return true;
        }
        if self.topology_dimension() == Some(Dimension::Surface) {
            match contact() {
                SegmentContact::Cross => return false,
                // No boundary contact: covers is exactly `no candidate
                // part in the exterior` (no interior-meet requirement).
                SegmentContact::None => return !exterior_part_uniform(other, self),
                SegmentContact::Touch => {},
            }
        }
        native_relate_shapes(self, other).is_covers()
    }

    pub fn covered_by(&self, other: &Self) -> bool {
        other.covers(self)
    }

    pub fn intersects(&self, other: &Self) -> bool {
        let Some(left) = self.bounds() else {
            return false;
        };
        let Some(right) = other.bounds() else {
            return false;
        };
        if !left.intersects(right) {
            return false;
        }
        // Two axis-aligned rectangles each equal their bounding box, so
        // overlapping bounds is already the exact (boundary-inclusive) answer —
        // the common box / tile / envelope case, which GEOS special-cases too.
        if self.is_axis_aligned_rectangle(left) && other.is_axis_aligned_rectangle(right) {
            return true;
        }
        intersection_contact(self, other)
    }

    /// Whether this shape is an axis-aligned rectangle: a hole-free polygon
    /// whose four distinct corners are exactly the corners of `bounds` (the
    /// caller's already-computed envelope). Such a shape is identical to its
    /// bounding box, so box tests answer predicates against it exactly. The
    /// `bounds` argument avoids recomputing the envelope. Cheap and
    /// early-outs (non-polygons, holed, or non-quad rings) so it never slows
    /// the general path.
    // Exact float equality is intentional and correct: `bounds` is the exact
    // min/max of these very vertices, so a corner is bit-identical to the
    // extreme it achieves. An epsilon would misclassify a near-rectangle and
    // hand back a wrong fast-path answer.
    #[expect(
        clippy::float_cmp,
        reason = "bounds are the exact min/max of these vertices"
    )]
    pub(crate) fn is_axis_aligned_rectangle(&self, bounds: Bounds) -> bool {
        let Self::Polygon(polygon) = self else {
            return false;
        };
        if !polygon.holes.is_empty() {
            return false;
        }
        let shell = polygon.shell.coords();
        // A closed rectangle ring is exactly 5 coords (4 corners + the repeat).
        if shell.coord_count() != 5
            || bounds.minx() == bounds.maxx()
            || bounds.miny() == bounds.maxy()
        {
            return false;
        }
        // Each of the four corners must sit on a distinct bbox corner; once all
        // four are present the ring is the rectangle. `bounds` is the exact min/
        // max of these vertices, so the equality tests need no epsilon.
        let mut seen = [false; 4];
        for point in shell.points().take(4) {
            let col = if point.x == bounds.minx() {
                0
            } else if point.x == bounds.maxx() {
                1
            } else {
                return false;
            };
            let row = if point.y == bounds.miny() {
                0
            } else if point.y == bounds.maxy() {
                1
            } else {
                return false;
            };
            seen[col * 2 + row] = true;
        }
        seen.iter().all(|&corner| corner)
    }

    /// Visit one representative vertex per connected 1D/2D component (first
    /// vertex of each line/shell), short-circuiting on the first `true`.
    /// With no boundary contact between two geometries, each component is
    /// uniformly inside or outside the other, so its representative decides
    /// area containment for the whole component.
    pub(crate) fn any_component_representative(
        &self,
        predicate: &mut impl FnMut(Point) -> bool,
    ) -> bool {
        match self {
            Self::Point(point) => predicate(*point),
            Self::MultiPoint(points) => points.iter().any(&mut *predicate),
            Self::LineString(points) => points.first().is_some_and(&mut *predicate),
            Self::MultiLineString(lines) => lines
                .iter()
                .any(|line| line.first().is_some_and(&mut *predicate)),
            Self::Polygon(polygon) => polygon.shell.first().is_some_and(&mut *predicate),
            Self::MultiPolygon(polygons) => polygons
                .iter()
                .any(|polygon| polygon.shell.first().is_some_and(&mut *predicate)),
            Self::GeometryCollection(geometries) => geometries
                .iter()
                .any(|geometry| geometry.any_component_representative(predicate)),
            Self::Empty(..) => false,
        }
    }

    /// Whether any part carries area (polygon parts, recursively through
    /// collections) — gates the representative-containment phase.
    pub(crate) fn has_area_parts(&self) -> bool {
        match self {
            Self::Polygon(_) | Self::MultiPolygon(_) => true,
            Self::GeometryCollection(geometries) => geometries.iter().any(Self::has_area_parts),
            _ => false,
        }
    }

    /// Boundary-inclusive point-in-AREA test: `covers_point` restricted to
    /// the polygon parts (1D/0D parts cannot contain a disjoint component).
    pub(crate) fn area_covers_point(&self, point: Point) -> bool {
        match self {
            Self::Polygon(polygon) => polygon.covers_point(point),
            Self::MultiPolygon(polygons) => {
                polygons.iter().any(|polygon| polygon.covers_point(point))
            },
            Self::GeometryCollection(geometries) => geometries
                .iter()
                .any(|geometry| geometry.area_covers_point(point)),
            _ => false,
        }
    }

    pub fn disjoint(&self, other: &Self) -> bool {
        !self.intersects(other)
    }

    pub fn touches(&self, other: &Self) -> bool {
        self.touches_with_bounds(other, self.bounds(), other.bounds(), || {
            linework_contact(self, other)
        })
    }

    pub(crate) fn touches_with_bounds(
        &self,
        other: &Self,
        own: Option<Bounds>,
        theirs: Option<Bounds>,
        contact: impl FnOnce() -> SegmentContact,
    ) -> bool {
        if has_collection_operand(self, other) {
            return native_relate_shapes(self, other).is_touches();
        }
        // Boundary contact WITHOUT interior overlap — ONE contact-
        // classified lane for every dimension pair.
        if option_bounds_disjoint(own, theirs) {
            return false;
        }
        // Interiors overlapping (a vertex STRICTLY inside the other) ⟹ NOT
        // touches. A cheap raycast refutes the common overlap case BEFORE
        // `contact()` pays to build the prepared linework (facet BVH). Strict
        // interior only, so a boundary-touch vertex never trips it.
        if strict_interior_witness(self, other) {
            return false;
        }
        match contact() {
            // A transversal crossing forces the interiors to meet (see
            // `linework_contact`; per part for collections — a collection
            // interior is the union of its part interiors).
            SegmentContact::Cross => false,
            // No linework contact: interiors meeting excludes touching,
            // and the only contact left is a bare point part resting ON
            // the other's boundary — covered but not contained, which IS
            // the OGC touch. Exact for every dimension; the matrix never
            // runs.
            SegmentContact::None => {
                !interiors_meet_uniform(self, other)
                    && (isolated_point_contact(self, other) || isolated_point_contact(other, self))
            },
            // Tangential contact: a vertex strictly inside the other's
            // area still refutes cheaply; then the native matrix grades
            // the tangency (geo only for collections).
            SegmentContact::Touch => {
                if strict_interior_witness(self, other) {
                    return false;
                }
                native_relate_shapes(self, other).is_touches()
            },
        }
    }

    pub fn crosses(&self, other: &Self) -> bool {
        self.crosses_with_bounds(other, self.bounds(), other.bounds(), || {
            linework_contact(self, other)
        })
    }

    pub(crate) fn crosses_with_bounds(
        &self,
        other: &Self,
        own: Option<Bounds>,
        theirs: Option<Bounds>,
        contact: impl FnOnce() -> SegmentContact,
    ) -> bool {
        if has_collection_operand(self, other) {
            return native_relate_shapes(self, other).is_crosses_by_dimension();
        }
        // A single Point never crosses anything (OGC; MultiPoint can). Kind
        // gate — not topological dimension — so MultiPoint×Area stays real.
        if matches!(self, Self::Point(_)) || matches!(other, Self::Point(_)) {
            return false;
        }
        // OGC crosses exists only where the intersection can sit BELOW
        // both operands' dimensions: point/line, point/area, line/line,
        // line/area. Puntal x puntal and areal x areal are false by
        // DEFINITION — no geometry inspected (GEOS answers these the same
        // way) — and disjoint bounds cannot cross anything.
        let (Some(left), Some(right)) = (self.topology_dimension(), other.topology_dimension())
        else {
            return false;
        };
        if left == right && left != Dimension::Curve {
            return false;
        }
        if option_bounds_disjoint(own, theirs) {
            return false;
        }
        if left.min(right) == Dimension::Curve && left.max(right) == Dimension::Surface {
            // Line/area crosses is `II ∧ int(line)∩ext(area)` (JTS grades
            // both operand orders symmetrically). A transversal crossing
            // through the area's boundary witnesses BOTH entries at once;
            // with no boundary contact each line component sits uniformly
            // on one side, so representatives decide both entries exactly.
            let (line, area) = if left == Dimension::Curve {
                (self, other)
            } else {
                (other, self)
            };
            match contact() {
                SegmentContact::Cross => return true,
                SegmentContact::None => {
                    return interior_part_uniform(line, area) && exterior_part_uniform(line, area);
                },
                SegmentContact::Touch => {
                    if let Some(matrix) = mixed_relate_shapes(self, other) {
                        return matrix.is_crosses_mixed(left == Dimension::Curve);
                    }
                },
            }
        } else if !intersection_contact(self, other) {
            return false;
        } else if let Some(matrix) = lineal_relate_shapes(self, other) {
            return matrix.is_crosses_lineal();
        }
        native_relate_shapes(self, other).is_crosses_by_dimension()
    }

    pub fn overlaps(&self, other: &Self) -> bool {
        self.overlaps_with_bounds(other, self.bounds(), other.bounds(), || {
            linework_contact(self, other)
        })
    }

    pub(crate) fn overlaps_with_bounds(
        &self,
        other: &Self,
        own: Option<Bounds>,
        theirs: Option<Bounds>,
        contact: impl FnOnce() -> SegmentContact,
    ) -> bool {
        if has_collection_operand(self, other) {
            return native_relate_shapes(self, other).is_overlaps_by_dimension();
        }
        // A single Point never overlaps anything (OGC; MultiPoint can).
        if matches!(self, Self::Point(_)) || matches!(other, Self::Point(_)) {
            return false;
        }
        // OGC overlaps requires EQUAL dimensions (mixed pairs are false by
        // definition) and disjoint bounds cannot overlap. Areal pairs ride
        // the contact classifier: only tangential contact ever reaches the
        // matrix.
        let (Some(left), Some(right)) = (self.topology_dimension(), other.topology_dimension())
        else {
            return false;
        };
        if left != right || option_bounds_disjoint(own, theirs) {
            return false;
        }
        if left == Dimension::Surface {
            match contact() {
                // A transversal boundary crossing makes the interiors meet
                // AND gives each one a quadrant outside the other — the
                // whole `T*T***T**` pattern at once (see
                // `linework_contact`).
                SegmentContact::Cross => return true,
                // No boundary contact: representatives decide the three
                // pattern entries (II, IE, EI) exactly.
                SegmentContact::None => {
                    return interiors_meet_uniform(self, other)
                        && exterior_part_uniform(self, other)
                        && exterior_part_uniform(other, self);
                },
                // Tangential contact: the four strict-side witnesses — a
                // vertex of each operand strictly inside AND strictly
                // outside the other — still prove a true overlap before
                // the matrix grades the tangency.
                SegmentContact::Touch => {
                    if vertex_witness(self, |point| other.contains_point(point))
                        && vertex_witness(self, |point| !other.covers_point(point))
                        && vertex_witness(other, |point| self.contains_point(point))
                        && vertex_witness(other, |point| !self.covers_point(point))
                    {
                        return true;
                    }
                },
            }
        } else if !intersection_contact(self, other) {
            return false;
        } else if let Some(matrix) = lineal_relate_shapes(self, other) {
            return matrix.is_overlaps_lineal();
        }
        native_relate_shapes(self, other).is_overlaps_by_dimension()
    }

    /// Topological dimension of the geometry's CONTENT (0 puntal, 1
    /// lineal, 2 areal; collections take the maximum of their parts);
    /// `None` when empty.
    pub(crate) fn topology_dimension(&self) -> Option<Dimension> {
        dimension(self, DimMode::Topology)
    }

    pub fn relate(&self, other: &Self) -> String {
        // Geographic pole/±180 seam: grade from the unsplit container (same
        // tri-state as `try_geographic_point_membership`). Split-only relate
        // mislabels seam interiors as fabricated-boundary contact.
        if let Some(matrix) = geographic_point_relate_matrix(self, other) {
            return matrix;
        }
        // Bbox-disjoint, both non-empty, non-collection: the DE-9IM is fully
        // determined by each operand's interior + boundary dimensions — no
        // noding/arrangement engine. (Empties and collections keep the general
        // path, where their matrices are special.)
        if let (Some(left), Some(right)) = (self.bounds(), other.bounds())
            && !left.intersects(right)
            && let Some(matrix) = disjoint_de9im(self, other)
        {
            return matrix.text();
        }
        native_relate_shapes(self, other).text()
    }
}

/// Point×container DE-9IM for pole / ±180 seam membership on the unsplit
/// container. Returns `None` when the pair is not a geographic pole/seam
/// probe (caller falls through to planar relate).
pub(crate) fn geographic_point_relate_matrix(left: &Shape, right: &Shape) -> Option<String> {
    use crate::geometry::PolePosition::{Boundary, Exterior, Interior};

    let (container, point, point_is_right) = match (left, right) {
        (container, Shape::Point(point)) => (container, *point, true),
        (Shape::Point(point), container) => (container, *point, false),
        _ => return None,
    };
    if !container.has_area_parts() {
        return None;
    }
    let position = if let Some(north) = point_is_geographic_pole(point) {
        pole_position(container, north)
    } else if point.x.to_bits() == (-180.0_f64).to_bits()
        || point.x.to_bits() == 180.0_f64.to_bits()
    {
        // Exact ±180 seam longitudes (bit identity — not a fuzzy compare).
        // Seam probe: works on both the original crossing shape and the
        // already-split multipolygon that `geo_split_pair` hands to relate.
        // (After split, `crosses_antimeridian` is false, so a gate that
        // required it would miss and native relate would grade fabricated
        // seam edges as boundary.)
        let probe = if container.crosses_antimeridian() {
            topology_split(container)
        } else {
            container.clone()
        };
        let west = point.with_xy((-180.0_f64).next_up(), point.y).ok()?;
        let east = point.with_xy(180.0_f64.next_down(), point.y).ok()?;
        let west_seam = point.with_xy(-180.0, point.y).ok()?;
        let east_seam = point.with_xy(180.0, point.y).ok()?;
        let seam_covered = probe.covers_point(west_seam) || probe.covers_point(east_seam);
        // Interior iff both sides of the meridian are in the areal interior
        // (or the point itself is strictly contained — covers-but-not-on-
        // fabricated-boundary after split still counts).
        let area_interior = (probe.contains_point(west) && probe.contains_point(east))
            || (seam_covered && probe.contains_point(point));
        if !seam_covered && !probe.covers_point(point) {
            // Not a seam-relevant container for this longitude — fall through.
            return None;
        }
        if !seam_covered {
            Exterior
        } else if area_interior {
            Interior
        } else {
            Boundary
        }
    } else {
        return None;
    };
    // Point-in-polygon DE-9IM (shapely/JTS): interior 0F2FF1FF2, boundary
    // FF20F1FF2, exterior FF2FF10F2.
    let matrix = match position {
        Interior => "0F2FF1FF2",
        Boundary => "FF20F1FF2",
        Exterior => "FF2FF10F2",
    };
    Some(if point_is_right {
        matrix.to_owned()
    } else {
        let b = matrix.as_bytes();
        String::from_utf8_lossy(&[b[0], b[3], b[6], b[1], b[4], b[7], b[2], b[5], b[8]])
            .into_owned()
    })
}

impl Shape {
    pub fn relate_pattern(&self, other: &Self, pattern: &str) -> Result<bool> {
        // Canonical single-pattern predicates dispatch to the fast kernels
        // (GEOS's RelateNG does the same): each listed pattern IS the
        // exact OGC definition of the predicate it routes to, so the
        // verdicts are identical to evaluating the full matrix.
        match pattern {
            "FF*FF****" => return Ok(!self.intersects(other)),
            "T*****FF*" => return Ok(self.contains(other)),
            "T*F**F***" => return Ok(self.within(other)),
            "T*F**FFF*" => return Ok(self.equals(other)),
            _ => {},
        }
        let compiled = relate_ng::CompiledPattern::new(pattern)
            .ok_or_else(|| Self::invalid_de9im_pattern(pattern))?;
        Ok(self.relate_pattern_compiled(other, compiled))
    }

    pub(crate) fn relate_pattern_compiled(
        &self,
        other: &Self,
        compiled: relate_ng::CompiledPattern<'_>,
    ) -> bool {
        // Bbox-disjoint short-circuit (mirrors `relate`): the DE-9IM is fully
        // determined by each operand's dimensions, so match the pattern against
        // it directly — no noding/arrangement engine. Without this, every
        // non-canonical pattern (e.g. the OGC `intersects` pattern `T********`)
        // paid the full relate, costing more than `relate` itself on
        // disjoint-heavy data.
        if let (Some(left), Some(right)) = (self.bounds(), other.bounds())
            && !left.intersects(right)
            && let Some(matrix) = disjoint_de9im(self, other)
        {
            return compiled.matches(matrix);
        }
        native_relate_pattern_shapes(self, other, compiled)
    }

    fn invalid_de9im_pattern(pattern: &str) -> crate::error::Error {
        GeometryErrorKind::invalid_de9im_pattern(format!(
            "invalid DE-9IM pattern {pattern:?}; expected 9 characters of \
             'T', 'F', '0', '1', '2', or '*'"
        ))
    }

    pub fn equals(&self, other: &Self) -> bool {
        self.equals_with_bounds(other, self.bounds(), other.bounds())
    }

    pub(crate) fn equals_with_bounds(
        &self,
        other: &Self,
        own: Option<Bounds>,
        theirs: Option<Bounds>,
    ) -> bool {
        // Topological equality treats every empty geometry as equal to every
        // other empty one regardless of kind (`POINT EMPTY equals POLYGON EMPTY`),
        // matching GEOS/Shapely. Handle it explicitly: the geo-rs relate graph is
        // not built for empty inputs.
        match (self.is_empty(), other.is_empty()) {
            (true, true) => return true,
            (true, false) | (false, true) => return false,
            (false, false) => {},
        }
        match (own, theirs) {
            (Some(left), Some(right)) if !bounds_equal_topological(left, right) => return false,
            (Some(_), None) | (None, Some(_)) => return false,
            _ => {},
        }
        // Bit-identical XY linework is topologically equal — one
        // early-exit columnar compare answers the duplicate/self
        // comparisons that otherwise pay the full intersection matrix
        // (Z/M are topology-irrelevant, so they stay out of the test).
        if self.equals_exact(other, 0.0, false, false) {
            return true;
        }
        native_relate_shapes(self, other).is_equal_topo()
    }

    /// Coordinate equality within `tolerance` (per-ordinate `|Δ| ≤ tolerance`,
    /// JTS/Shapely `equals_exact` semantics): the structure must match and
    /// every paired ordinate must agree to `tolerance`. `tolerance == 0.0`
    /// is exact. CRS/epoch are ignored here — that is the job of `==`.
    pub fn equals_exact(
        &self,
        other: &Self,
        tolerance: f64,
        include_z: bool,
        include_m: bool,
    ) -> bool {
        match (include_z, include_m) {
            (false, false) => self.equals_exact_impl::<false, false>(other, tolerance),
            (true, false) => self.equals_exact_impl::<true, false>(other, tolerance),
            (false, true) => self.equals_exact_impl::<false, true>(other, tolerance),
            (true, true) => self.equals_exact_impl::<true, true>(other, tolerance),
        }
    }

    pub(crate) fn equals_exact_impl<const Z: bool, const M: bool>(
        &self,
        other: &Self,
        tolerance: f64,
    ) -> bool {
        match (self, other) {
            (Self::Point(left), Self::Point(right)) => {
                point_equals_exact::<Z, M>(*left, *right, tolerance)
            },
            (Self::MultiPoint(left), Self::MultiPoint(right)) => {
                points_equal_exact::<Z, M, _, _>(left, right, tolerance)
            },
            (Self::LineString(left), Self::LineString(right)) => {
                points_equal_exact::<Z, M, _, _>(left, right, tolerance)
            },
            (Self::MultiLineString(left), Self::MultiLineString(right)) => {
                left.len() == right.len()
                    && left.iter().zip(right).all(|(left, right)| {
                        points_equal_exact::<Z, M, _, _>(left, right, tolerance)
                    })
            },
            (Self::Polygon(left), Self::Polygon(right)) => {
                left.equals_exact_impl::<Z, M>(right, tolerance)
            },
            (Self::MultiPolygon(left), Self::MultiPolygon(right)) => {
                left.len() == right.len()
                    && left
                        .iter()
                        .zip(right)
                        .all(|(left, right)| left.equals_exact_impl::<Z, M>(right, tolerance))
            },
            (Self::GeometryCollection(left), Self::GeometryCollection(right)) => {
                left.len() == right.len()
                    && left
                        .iter()
                        .zip(right)
                        .all(|(left, right)| left.equals_exact_impl::<Z, M>(right, tolerance))
            },
            // Structural equality: a typed empty matches only the same-kind,
            // same-declared-axes empty (`POINT EMPTY` ≠ `POLYGON EMPTY`,
            // `POINT EMPTY` ≠ `POINT Z EMPTY`), unlike topological `equals`.
            // The Z/M axes comparisons honor `include_z`/`include_m`, exactly
            // like the column-presence comparison for empty sequences.
            (Self::Empty(left, left_axes), Self::Empty(right, right_axes)) => {
                left == right
                    && (!Z || left_axes.has_z() == right_axes.has_z())
                    && (!M || left_axes.has_m() == right_axes.has_m())
            },
            // Cross-representation: an axes-tagged container empty against the
            // same kind's canonical XY empty-`Vec` form. The container form is
            // always XY (`typed_empty` normalization), so the tagged side must
            // have no INCLUDED extra axes to match.
            (Self::Empty(kind, axes), other) | (other, Self::Empty(kind, axes)) => {
                let container_empty = match (kind, other) {
                    (EmptyKind::MultiLineString, Self::MultiLineString(lines)) => lines.is_empty(),
                    (EmptyKind::MultiPolygon, Self::MultiPolygon(polygons)) => polygons.is_empty(),
                    (EmptyKind::GeometryCollection, Self::GeometryCollection(geometries)) => {
                        geometries.is_empty()
                    },
                    _ => false,
                };
                container_empty && (!Z || !axes.has_z()) && (!M || !axes.has_m())
            },
            _ => false,
        }
    }
}
