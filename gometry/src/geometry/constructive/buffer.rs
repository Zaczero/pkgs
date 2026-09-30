use std::num::NonZeroU32;

#[cfg(test)]
use crate::geometry::DEFAULT_MITER_LIMIT;
use crate::geometry::constructive::{
    Result, WalkJoinRule, convex_buffer_budgeted, degenerate_polygonal_as_linework,
    multipoint_loops, point_buffer, validate_buffer_style, winding_buffer_budgeted,
    winding_collection_budgeted, winding_erosion_budgeted, winding_region,
    winding_sided_stroke_budgeted, winding_stroke_budgeted,
};
use crate::geometry::{
    BufferCapStyle, BufferJoinStyle, BufferSide, CoordSeq, ExpansionBudget, GeometryErrorKind,
    LineSeq, Shape, polygon_parts_to_shape, same_topological_coordinate,
};
use crate::{Finite, Positive};

impl Shape {
    #[cfg(test)]
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
                Self::Point(_)
                | Self::MultiPoint(_)
                | Self::LineString(_)
                | Self::MultiLineString(_)
                | Self::Empty(..) => Ok(Some(Self::empty_polygon())),
            };
        }
        match self {
            Self::Point(point) => {
                point_buffer(*point, distance, quadrant_segments, budget).map(Some)
            },
            Self::MultiPoint(points) => {
                let mut loops = Vec::with_capacity(points.len());
                multipoint_loops(&mut loops, points, distance, quadrant_segments, budget)?;
                let parts = winding_region(&loops, |winding| winding >= 1)?;
                if parts.is_empty() {
                    return Ok(None);
                }
                Ok(Some(polygon_parts_to_shape(parts)))
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

    /// One-sided buffer of lineal geometry, with flat ends and styled joins.
    /// Symmetric (`side='both'`) buffers take the regular
    /// [`Self::buffer_with_style`] path at the caller.
    pub(crate) fn buffer_sided(
        &self,
        distance: f64,
        side: BufferSide,
        join_style: BufferJoinStyle,
        quadrant_segments: NonZeroU32,
        miter_limit: Positive,
    ) -> Result<Self> {
        let mut budget = ExpansionBudget::new("buffer", "quadrant_segments");
        self.buffer_sided_budgeted(
            distance,
            side,
            join_style,
            quadrant_segments,
            miter_limit,
            &mut budget,
        )
    }

    pub(crate) fn buffer_sided_budgeted(
        &self,
        distance: f64,
        side: BufferSide,
        join_style: BufferJoinStyle,
        quadrant_segments: NonZeroU32,
        miter_limit: Positive,
        budget: &mut ExpansionBudget,
    ) -> Result<Self> {
        debug_assert!(
            side != BufferSide::Both,
            "callers route 'both' to buffer_with_style"
        );
        let distance = Finite::try_new("distance", distance)?.get();
        if distance < 0.0 {
            return Err(GeometryErrorKind::NegativeSidedBufferDistance.into());
        }
        validate_buffer_style(quadrant_segments, miter_limit);
        let rule = WalkJoinRule::new(join_style, miter_limit.get());
        let mut chains = Vec::new();
        let mut stack = vec![self];
        while let Some(geometry) = stack.pop() {
            match geometry {
                Self::LineString(line) => chains.push(line.as_coords()),
                Self::MultiLineString(lines) => {
                    chains.extend(lines.iter().map(LineSeq::as_coords));
                },
                Self::GeometryCollection(parts) => stack.extend(parts.iter().rev()),
                Self::Empty(..) => {},
                Self::Point(_) | Self::MultiPoint(_) | Self::Polygon(_) | Self::MultiPolygon(_) => {
                    return Err(GeometryErrorKind::SidedBufferRequiresLineal.into());
                },
            }
        }
        if chains.is_empty() || same_topological_coordinate(distance, 0.0) {
            return Ok(Self::empty_polygon());
        }
        let signed = match side {
            BufferSide::Left => distance,
            BufferSide::Right | BufferSide::Both => -distance,
        };
        winding_sided_stroke_budgeted(&chains, signed, rule, quadrant_segments, budget)
    }
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
            Shape::Point(_)
            | Shape::MultiPoint(_)
            | Shape::LineString(_)
            | Shape::MultiLineString(_)
            | Shape::Polygon(_)
            | Shape::MultiPolygon(_) => {
                if !part.is_empty() && only.replace(part).is_some() {
                    return None;
                }
            },
            Shape::Empty(..) => {},
        }
    }
    only
}
