use std::num::NonZeroU32;

use super::*;
use crate::{Finite, Positive};

impl Shape {
    pub fn buffer(&self, distance: f64) -> Result<Self> {
        self.buffer_with_style(
            distance,
            BufferCapStyle::Round,
            BufferJoinStyle::Round,
            NonZeroU32::new(8).expect("8 is non-zero"),
            Positive::try_new("miter_limit", DEFAULT_MITER_LIMIT)?,
        )
    }

    pub(crate) fn buffer_with_style(
        &self,
        distance: f64,
        cap_style: BufferCapStyle,
        join_style: BufferJoinStyle,
        quadrant_segments: NonZeroU32,
        miter_limit: Positive,
    ) -> Result<Self> {
        let distance = Finite::try_new("distance", distance)?.get();
        // Buffering any empty geometry yields the empty polygon (Shapely
        // semantics); short-circuit before handing geo-rs an empty input.
        if self.is_empty() {
            return Ok(Self::empty_polygon());
        }
        validate_buffer_style(quadrant_segments, miter_limit);
        let miter_limit = miter_limit.get();
        let rule = WalkJoinRule::new(join_style, miter_limit);
        if same_topological_coordinate(distance, 0.0)
            && matches!(self, Self::Polygon(_) | Self::MultiPolygon(_))
            && self.validate().is_none()
        {
            return Ok(self.clone());
        }
        validate_buffer_expansion(self, quadrant_segments)?;
        // GEOS-style input reduction (matches `BufferInputLineSimplifier`):
        // an input vertex whose deviation is a tiny fraction of the buffer
        // distance cannot move the offset result beyond its own arc-faceting
        // error, so dropping such vertices shrinks the noding/dissolve workload
        // — and the output — proportionally, with no material shape change. A
        // smooth high-vertex ring/line collapses to the few vertices that
        // actually shape the buffer; small inputs keep every vertex (their
        // detail exceeds the tolerance) and are untouched. The win is dramatic
        // for lineal strokes whose self-overlapping capsule the winding
        // arrangement would otherwise resolve over every original segment.
        // Polygons reduce for growth AND erosion; lineal input only for a
        // positive distance (a non-positive distance annihilates it); `buffer(0)`
        // must reproduce the exact valid region, so it never reduces.
        let needs_reduction = match self {
            Self::Polygon(_) | Self::MultiPolygon(_) => distance != 0.0,
            Self::LineString(_) | Self::MultiLineString(_) => distance > 0.0,
            _ => false,
        };
        let reduced;
        let source = if needs_reduction {
            reduced = self.simplify_dp_raw(distance.abs() * BUFFER_INPUT_SIMPLIFY_FACTOR)?;
            &reduced
        } else {
            self
        };
        // Convex hole-free polygons buffer constructively (offset edges +
        // styled joins, no boolean resolution) — see `convex_buffer`.
        if let Self::Polygon(polygon) = source
            && let Some(fast) = convex_buffer(polygon, distance, rule, quadrant_segments)
        {
            return Ok(fast);
        }
        // The winding engine routes every real geometry — polygons expand
        // and erode, chains stroke (zero-length chains become disks),
        // points are disks, and a whole GeometryCollection reduces to ONE
        // shared arrangement. Non-positive distances annihilate
        // puntal/lineal input exactly, and `buffer(0)` of polygonal input
        // is the winding selection of the unmoved rings — the d -> 0
        // limit (the valid region; one lobe of a bowtie, like GEOS).
        // `None` means the polygonal input has degenerate (zero-area) rings,
        // which have no interior to expand or erode.
        if let Some(result) = source.winding_route(distance, cap_style, rule, quadrant_segments) {
            return Ok(result);
        }
        // A zero-area polygon IS its boundary linework: reinterpret its rings
        // as lines and buffer those. A positive distance strokes them into
        // capsules (a single coincident-point ring becomes a disk); a
        // non-positive distance annihilates them, matching the empty erosion
        // of a region with no interior. This is the principled native answer
        // for inputs GEOS itself flags `invalid` and resolves the same way.
        let linework = degenerate_polygonal_as_linework(source);
        Ok(linework
            .winding_route(distance, cap_style, rule, quadrant_segments)
            .unwrap_or_else(Self::empty_polygon))
    }

    /// Route one shape through the winding buffer engine (see
    /// [`buffer_with_style`](Self::buffer_with_style); `None` falls back
    /// to the geo engine).
    pub(crate) fn winding_route(
        &self,
        distance: f64,
        cap_style: BufferCapStyle,
        rule: WalkJoinRule,
        quadrant_segments: NonZeroU32,
    ) -> Option<Self> {
        if distance <= 0.0 {
            // Erosion at distance 0 selects `winding <= -1` of the
            // UNMOVED flipped rings — exactly the valid region, so
            // `buffer(0)` is the d -> 0 limit for free.
            return match self {
                Self::Polygon(polygon) => winding_erosion(
                    std::slice::from_ref(polygon),
                    -distance,
                    rule,
                    quadrant_segments,
                ),
                Self::MultiPolygon(polygons) => {
                    winding_erosion(polygons, -distance, rule, quadrant_segments)
                },
                Self::GeometryCollection(parts) => {
                    winding_collection(parts, distance, cap_style, rule, quadrant_segments)
                },
                // Non-positive distances annihilate puntal/lineal input
                // exactly (GEOS semantics).
                _ => Some(Self::empty_polygon()),
            };
        }
        match self {
            Self::Point(point) => point_buffer(*point, distance, quadrant_segments),
            Self::MultiPoint(points) => {
                let mut loops = Vec::with_capacity(points.len());
                for point in points.iter_coords() {
                    loops.push(circle_loop(point, distance, quadrant_segments)?);
                }
                let parts = winding_region(&loops, |winding| winding >= 1);
                if parts.is_empty() {
                    return None;
                }
                Some(polygon_parts_to_shape(parts))
            },
            Self::LineString(chain) => {
                winding_stroke(&[chain], distance, cap_style, rule, quadrant_segments)
            },
            Self::MultiLineString(lines) => {
                let chains: Vec<&CoordSeq> = lines.iter().map(LineSeq::as_coords).collect();
                winding_stroke(&chains, distance, cap_style, rule, quadrant_segments)
            },
            Self::Polygon(polygon) => winding_buffer(
                std::slice::from_ref(polygon),
                distance,
                rule,
                quadrant_segments,
            ),
            Self::MultiPolygon(polygons) => {
                winding_buffer(polygons, distance, rule, quadrant_segments)
            },
            Self::GeometryCollection(parts) => {
                // A collection with one real member buffers AS that member
                // (the convex/point fast paths apply); mixed collections
                // share one arrangement.
                let mut real = parts.iter().filter(|part| !part.is_empty());
                match (real.next(), real.next()) {
                    (Some(only), None) => {
                        only.winding_route(distance, cap_style, rule, quadrant_segments)
                    },
                    _ => winding_collection(parts, distance, cap_style, rule, quadrant_segments),
                }
            },
            Self::Empty(..) => Some(Self::empty_polygon()),
        }
    }

    /// One-sided buffer of lineal geometry: the strip between each line and
    /// its offset curve at `distance` on `side` (left/right of the line
    /// direction), with flat ends and miter joins — the `offset_curve`
    /// construction closed into a polygon. Symmetric (`side='both'`) buffers
    /// take the regular [`Self::buffer_with_style`] path at the caller.
    pub fn buffer_sided(&self, distance: f64, side: BufferSide) -> Result<Self> {
        debug_assert!(
            side != BufferSide::Both,
            "callers route 'both' to buffer_with_style"
        );
        let distance = Finite::try_new("distance", distance)?.get();
        if distance < 0.0 {
            return Err(GeometryErrorKind::NegativeSidedBufferDistance.into());
        }
        if self.is_empty() {
            return Ok(Self::empty_polygon());
        }
        // Positive offsets fall on the left of the line direction (the
        // `offset_curve` convention); the right side mirrors the sign.
        let signed = match side {
            BufferSide::Left => distance,
            _ => -distance,
        };
        let strips = match self {
            Self::LineString(points) => sided_strip_parts(points, signed)?,
            Self::MultiLineString(lines) => {
                let mut parts = Vec::new();
                for line in lines {
                    parts.extend(sided_strip_parts(line, signed)?);
                }
                parts
            },
            Self::GeometryCollection(geometries) => {
                let parts = geometries
                    .iter()
                    .map(|geometry| geometry.buffer_sided(distance, side))
                    .collect::<Result<Vec<_>>>()?;
                let mut polygons = Vec::new();
                for part in parts {
                    match part {
                        Self::Polygon(polygon) => polygons.push(polygon),
                        Self::MultiPolygon(more) => polygons.extend(more),
                        _ => {},
                    }
                }
                polygons
            },
            _ => return Err(GeometryErrorKind::SidedBufferRequiresLineal.into()),
        };
        if strips.is_empty() || distance == 0.0 {
            return Ok(Self::empty_polygon());
        }
        // Per-line strips are individually valid; multi-part inputs can still
        // overlap each other, where the structure repair unions the parts
        // into a valid polygonal result (the same validity contract as
        // `buffer`). Buffer output is always X/Y-only — like the geo-backed
        // symmetric buffer, and matching what the repair lanes produce — so
        // the surfaces' synthesizing-op contract (`'auto'` yields 2D) holds on
        // every path.
        let shape = if strips.len() == 1 {
            Self::Polygon(strips.into_iter().next().expect("one strip"))
        } else {
            Self::MultiPolygon(strips)
        };
        let shape = shape.force_2d();
        if shape.validate().is_none() {
            Ok(shape)
        } else {
            shape.repair(RepairMethod::Structure, false)
        }
    }
}
