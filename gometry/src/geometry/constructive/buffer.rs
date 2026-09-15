use std::num::NonZeroU32;

use crate::geometry::constructive::{
    DEFAULT_MITER_LIMIT, Result, WalkJoinRule, convex_buffer_budgeted,
    degenerate_polygonal_as_linework, multipoint_loops, point_buffer, sided_strip_parts,
    validate_buffer_style, winding_buffer_budgeted, winding_collection_budgeted,
    winding_erosion_budgeted, winding_region, winding_stroke_budgeted,
};
use crate::geometry::{
    Bounds, BufferCapStyle, BufferJoinStyle, BufferSide, CoordSeq, ExpansionBudget,
    GeometryErrorKind, LineSeq, RepairMethod, Shape, for_each_overlapping_bounds_pair,
    polygon_parts_to_shape, same_topological_coordinate,
};
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
        let mut budget = ExpansionBudget::new("buffer", "quadrant_segments");
        self.buffer_with_style_budgeted(
            distance,
            cap_style,
            join_style,
            quadrant_segments,
            miter_limit,
            &mut budget,
        )
    }

    pub(crate) fn buffer_with_style_budgeted(
        &self,
        distance: f64,
        cap_style: BufferCapStyle,
        join_style: BufferJoinStyle,
        quadrant_segments: NonZeroU32,
        miter_limit: Positive,
        budget: &mut ExpansionBudget,
    ) -> Result<Self> {
        let distance = Finite::try_new("distance", distance)?.get();
        // Buffering any empty geometry yields the operation's typed empty
        // polygon; short-circuit before handing the empty input to the
        // constructive route.
        if self.is_empty() {
            return Ok(Self::empty_polygon());
        }
        validate_buffer_style(quadrant_segments, miter_limit);
        let miter_limit = miter_limit.get();
        let rule = WalkJoinRule::new(join_style, miter_limit);
        if same_topological_coordinate(distance, 0.0)
            && matches!(self, Self::Polygon(_))
            && self.validate()?.is_none()
        {
            return Ok(self.clone());
        }
        // The admitted geometry is the occupied support: buffering does not
        // run a tolerance-based input simplifier before constructing it.
        // Convex hole-free polygons buffer constructively (offset edges +
        // styled joins, no boolean resolution) — see `convex_buffer_budgeted`.
        if let Self::Polygon(polygon) = self
            && let Some(fast) =
                convex_buffer_budgeted(polygon, distance, rule, quadrant_segments, budget)?
        {
            return Ok(fast);
        }
        // The winding engine routes every real geometry — polygons expand
        // and erode, chains stroke (zero-length chains become disks),
        // points are disks, and a whole GeometryCollection reduces to ONE
        // shared arrangement. Non-positive distances annihilate
        // puntal/lineal input exactly, and `buffer(0)` of polygonal input
        // is the winding selection of the unmoved rings — the d -> 0
        // limit of the valid region, including the selected lobe of a bowtie.
        // `None` means the polygonal input has degenerate (zero-area) rings,
        // which have no interior to expand or erode.
        if let Some(result) =
            self.winding_route(distance, cap_style, rule, quadrant_segments, budget)?
        {
            return Ok(result);
        }
        // A zero-area polygon IS its boundary linework: reinterpret its rings
        // as lines and buffer those. A positive distance strokes them into
        // capsules (a single coincident-point ring becomes a disk); a
        // non-positive distance annihilates them, because a region with no
        // interior has nothing to erode.
        let linework = degenerate_polygonal_as_linework(self)?;
        Ok(linework
            .winding_route(distance, cap_style, rule, quadrant_segments, budget)?
            .unwrap_or_else(Self::empty_polygon))
    }

    /// Route one shape through the winding buffer engine (see
    /// [`buffer_with_style`](Self::buffer_with_style)); `None` means the
    /// constructive route has no usable loop.
    pub(crate) fn winding_route(
        &self,
        distance: f64,
        cap_style: BufferCapStyle,
        rule: WalkJoinRule,
        quadrant_segments: NonZeroU32,
        budget: &mut ExpansionBudget,
    ) -> Result<Option<Self>> {
        if distance <= 0.0 {
            // The erosion owner normalizes all selected polygonal support
            // before offsetting. At distance zero that normalization itself
            // is the occupied-support result; a valid simple Polygon was
            // already returned unchanged above.
            return match self {
                Self::Polygon(polygon) => winding_erosion_budgeted(
                    std::slice::from_ref(polygon),
                    -distance,
                    rule,
                    quadrant_segments,
                    budget,
                ),
                Self::MultiPolygon(polygons) => {
                    winding_erosion_budgeted(polygons, -distance, rule, quadrant_segments, budget)
                },
                Self::GeometryCollection(parts) => winding_collection_budgeted(
                    parts,
                    distance,
                    cap_style,
                    rule,
                    quadrant_segments,
                    budget,
                ),
                // Non-positive distances annihilate puntal/lineal input.
                _ => Ok(Some(Self::empty_polygon())),
            };
        }
        match self {
            Self::Point(point) => point_buffer(*point, distance, quadrant_segments, budget),
            Self::MultiPoint(points) => {
                let mut loops = Vec::new();
                crate::try_reserve_vec(&mut loops, points.len())?;
                if multipoint_loops(&mut loops, points, distance, quadrant_segments, budget)?
                    .is_none()
                {
                    return Ok(None);
                }
                let parts = winding_region(&loops, |winding| winding >= 1)?;
                if parts.is_empty() {
                    return Ok(None);
                }
                Ok(Some(polygon_parts_to_shape(parts)?))
            },
            Self::LineString(chain) => winding_stroke_budgeted(
                &[chain],
                distance,
                cap_style,
                rule,
                quadrant_segments,
                budget,
            ),
            Self::MultiLineString(lines) => {
                let chains: Vec<&CoordSeq> = lines.iter().map(LineSeq::as_coords).collect();
                winding_stroke_budgeted(
                    &chains,
                    distance,
                    cap_style,
                    rule,
                    quadrant_segments,
                    budget,
                )
            },
            Self::Polygon(polygon) => winding_buffer_budgeted(
                std::slice::from_ref(polygon),
                distance,
                rule,
                quadrant_segments,
                budget,
            ),
            Self::MultiPolygon(polygons) => {
                winding_buffer_budgeted(polygons, distance, rule, quadrant_segments, budget)
            },
            Self::GeometryCollection(parts) => {
                // A collection with one real member buffers AS that member
                // (the convex/point fast paths apply); mixed collections share
                // one arrangement.  Find that member across nested collections
                // in one iterative pass: recursively asking `is_empty()` at
                // every one-child wrapper rescanned the entire tail at each
                // level and made a deep collection quadratic.
                match sole_nonempty_collection_member(parts) {
                    Some(only) => {
                        only.winding_route(distance, cap_style, rule, quadrant_segments, budget)
                    },
                    _ => winding_collection_budgeted(
                        parts,
                        distance,
                        cap_style,
                        rule,
                        quadrant_segments,
                        budget,
                    ),
                }
            },
            Self::Empty(..) => Ok(Some(Self::empty_polygon())),
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
        let shape = Self::narrow_polygons(crate::geometry::CoordinateAxes::XY, strips);
        let shape = shape?.force_2d()?;
        if strips_are_pairwise_strictly_disjoint(&shape)? || shape.validate()?.is_none() {
            Ok(shape)
        } else {
            shape.repair(RepairMethod::Structure, false)
        }
    }
}

/// Individually valid strips with pairwise non-intersecting closed envelopes
/// cannot overlap or touch, which is exactly the remaining MultiPolygon
/// validity condition. The envelope sweep avoids a quadratic proof for large
/// multi-line inputs.
fn strips_are_pairwise_strictly_disjoint(shape: &Shape) -> crate::error::Result<bool> {
    let Shape::MultiPolygon(strips) = shape else {
        return Ok(true);
    };
    let mut bounds = Vec::with_capacity(strips.len());
    for strip in strips.iter() {
        let Some(bounds_for_strip) = Bounds::from_coords(strip.shell.coords()) else {
            return Ok(false);
        };
        bounds.push(bounds_for_strip);
    }
    Ok(
        for_each_overlapping_bounds_pair(&bounds, |_, _| std::ops::ControlFlow::Break(()))?
            .is_continue(),
    )
}

/// The only non-empty non-collection member of `parts`, if there is exactly
/// one.  An explicit stack keeps deeply nested one-child collections off the
/// Rust call stack and never asks a collection to recursively scan itself.
fn sole_nonempty_collection_member(parts: &[Shape]) -> Option<&Shape> {
    let mut stack: Vec<&Shape> = parts.iter().rev().collect();
    let mut only = None;
    while let Some(part) = stack.pop() {
        match part {
            Shape::GeometryCollection(nested) => stack.extend(nested.iter().rev()),
            _ if part.is_empty() => {},
            _ if only.replace(part).is_some() => return None,
            _ => {},
        }
    }
    only
}
