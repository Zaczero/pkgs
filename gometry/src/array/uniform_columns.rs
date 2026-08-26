//! The uniform-column packers behind `from_shapes`/`pack_or_mixed`:
//! one shared `CoordSeq` (+CSR offsets) when every row is the same
//! primitive kind with uniform axes, else `None` -> `Mixed`.

use crate::array::{
    CoordSeq, CoordinateAxes, CsrOffsetBuilder, CsrOffsetColumn, PolygonLevel, PyGeometryArray,
    RingLevel, Shape,
};
use crate::geometry::CoordSeqBuilder;

impl PyGeometryArray {
    /// `Some((coords, offsets))` when every row is a non-empty `LineString`
    /// of one shared axes layout — the packed-`Lines` gate. Bails (`None`)
    /// on any other row kind, mixed axes, or an empty input.
    pub(crate) fn uniform_line_column<'a>(
        shapes: impl ExactSizeIterator<Item = &'a Shape> + Clone,
    ) -> Option<(CoordSeq, CsrOffsetColumn)> {
        if shapes.len() == 0 {
            return None;
        }
        let mut offset_builder = CsrOffsetBuilder::new();
        let mut axes: Option<CoordinateAxes> = None;
        // Validate + total in one pass, then REPLAY the cloned iterator to
        // copy columns — no per-row `Vec<&CoordSeq>` stash between passes.
        let replay = shapes.clone();
        let mut total: usize = 0;
        let vertex_cap = i32::MAX as usize;
        for shape in shapes {
            let Shape::LineString(seq) = shape else {
                return None;
            };
            if seq.len() < 2 {
                return None;
            }
            let row_axes = seq.axes();
            match axes {
                None => axes = Some(row_axes),
                Some(existing) if existing != row_axes => return None,
                Some(_) => {},
            }
            total = total.checked_add(seq.len())?;
            if total > vertex_cap {
                return None;
            }
            offset_builder.push_end(total, vertex_cap).ok()?;
        }
        let Some(axes) = axes else {
            return None;
        };
        let mut builder = CoordSeqBuilder::try_with_capacity(axes, total).ok()?;
        for shape in replay {
            let Shape::LineString(seq) = shape else {
                unreachable!("validated in the first pass");
            };
            builder.extend_window(seq, 0..seq.len());
        }
        let coords = builder.finish().ok()?;
        let offsets = offset_builder.finish(total).ok()?;
        Some((coords, offsets))
    }

    /// The polygon sibling of [`uniform_line_column`]: every shape must be
    /// a `Polygon` with the same axes; rings gather (closed, as stored)
    /// into one column set under a two-level CSR. `None` falls back to
    /// `Mixed` (mixed types/axes, malformed rings, or offsets past the
    /// `i32` Arrow domain).
    ///
    /// Central pack-admission gate: every polygon must have a shell and every
    /// ring must have ≥4 XY-closed coordinates. Malformed WKB rings stay
    /// `Mixed` so packed kernels never panic on empty/short/unclosed spans.
    pub(crate) fn uniform_polygon_column<'a>(
        shapes: impl ExactSizeIterator<Item = &'a Shape> + Clone,
    ) -> Option<(
        CoordSeq,
        CsrOffsetColumn<RingLevel>,
        CsrOffsetColumn<PolygonLevel>,
    )> {
        if shapes.len() == 0 {
            return None;
        }
        let mut ring_builder = CsrOffsetBuilder::new();
        let mut polygon_builder = CsrOffsetBuilder::new();
        let mut axes: Option<CoordinateAxes> = None;
        // Same replay shape as `uniform_line_column`: no `Vec<&Polygon>` stash.
        let replay = shapes.clone();
        let mut total_vertices = 0_usize;
        let mut ring_index = 0_usize;
        let vertex_cap = i32::MAX as usize;
        for shape in shapes {
            let Shape::Polygon(polygon) = shape else {
                return None;
            };
            // Shared structural pack-admission (shell, ≥4 XY-closed rings,
            // uniform axes) — same predicate as streaming + packed GeoArrow.
            let Some(row_axes) = super::polygon_pack_axes(polygon) else {
                return None;
            };
            match axes {
                None => axes = Some(row_axes),
                Some(existing) if existing != row_axes => return None,
                Some(_) => {},
            }
            let mut poly_rings = 0_usize;
            for seq in polygon.rings() {
                debug_assert!(super::ring_seq_is_packable(seq));
                total_vertices = total_vertices.checked_add(seq.len())?;
                if total_vertices > vertex_cap {
                    return None;
                }
                ring_builder.push_end(total_vertices, vertex_cap).ok()?;
                ring_index = ring_index.checked_add(1)?;
                if ring_index > vertex_cap {
                    return None;
                }
                poly_rings = poly_rings.checked_add(1)?;
            }
            // polygon_pack_axes already requires a nonempty ring set.
            debug_assert!(poly_rings > 0);
            polygon_builder.push_end(ring_index, vertex_cap).ok()?;
        }
        let Some(axes) = axes else {
            return None;
        };
        let mut builder = CoordSeqBuilder::try_with_capacity(axes, total_vertices).ok()?;
        for shape in replay {
            let Shape::Polygon(polygon) = shape else {
                unreachable!("validated in the first pass");
            };
            for seq in polygon.rings() {
                builder.extend_window(seq, 0..seq.len());
            }
        }
        let coords = builder.finish().ok()?;
        let ring_offsets = ring_builder
            .finish(total_vertices)
            .ok()?
            .cast_level::<RingLevel>();
        let polygon_offsets = polygon_builder
            .finish(ring_index)
            .ok()?
            .cast_level::<PolygonLevel>();
        Some((coords, ring_offsets, polygon_offsets))
    }

    /// The shared point column when every shape is a `Point` of uniform axes,
    /// else `None`. Mixed-axis points stay `Mixed` — packing them into one
    /// `CoordSeq` would lie about per-row Z/M presence. The single packability
    /// rule behind [`from_shapes`](Self::from_shapes) and
    /// [`pack_or_mixed`](Self::pack_or_mixed).
    pub(crate) fn uniform_point_column<'a>(
        shapes: impl ExactSizeIterator<Item = &'a Shape> + Clone,
    ) -> Option<CoordSeq> {
        if shapes.len() == 0 {
            return None;
        }
        let mut axes: Option<CoordinateAxes> = None;
        for shape in shapes.clone() {
            let Shape::Point(point) = shape else {
                return None;
            };
            let point_axes = CoordinateAxes::from_point(*point);
            match axes {
                None => {
                    axes = Some(point_axes);
                },
                Some(existing) if existing != point_axes => return None,
                Some(_) => {},
            }
        }
        let Some(axes) = axes else {
            return None;
        };
        let mut builder = CoordSeqBuilder::try_with_capacity(axes, shapes.len()).ok()?;
        for shape in shapes {
            let Shape::Point(point) = shape else {
                unreachable!("validated in the first pass");
            };
            builder.push(*point);
        }
        Some(builder.finish().ok()?)
    }
}
