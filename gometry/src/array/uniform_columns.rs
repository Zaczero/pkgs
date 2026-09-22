//! The uniform-column packers behind `from_shapes`/`pack_or_mixed`:
//! one shared `CoordSeq` (+CSR offsets) when every row is the same
//! primitive kind with uniform axes, else `None` -> `Mixed`.

use crate::array::{
    CoordSeq, CoordinateAxes, CsrOffsetBuilder, CsrOffsetColumn, PolygonLevel, PyGeometryArray,
    RingLevel, Shape,
};
use crate::error::{ErrorKind, Result};
use crate::geometry::CoordSeqBuilder;

type PackedPolygonColumns = (
    CoordSeq,
    CsrOffsetColumn<RingLevel>,
    CsrOffsetColumn<PolygonLevel>,
);

fn admit_or_decline<T>(result: Result<T>) -> Result<Option<T>> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(error) if matches!(error.kind(), ErrorKind::Resource(_)) => Err(error),
        Err(_) => Ok(None),
    }
}

impl PyGeometryArray {
    /// `Some((coords, offsets))` when every row is a non-empty `LineString`
    /// of one shared axes layout — the packed-`Lines` gate. Bails (`None`)
    /// on any other row kind, mixed axes, or an empty input.
    pub(crate) fn uniform_line_column<'a>(
        shapes: impl ExactSizeIterator<Item = &'a Shape> + Clone,
    ) -> Result<Option<(CoordSeq, CsrOffsetColumn)>> {
        if shapes.len() == 0 {
            return Ok(None);
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
                return Ok(None);
            };
            if seq.len() < 2 {
                return Ok(None);
            }
            let row_axes = seq.axes();
            match axes {
                None => axes = Some(row_axes),
                Some(existing) if existing != row_axes => return Ok(None),
                Some(_) => {},
            }
            let Some(next) = total.checked_add(seq.len()) else {
                return Ok(None);
            };
            total = next;
            if total > vertex_cap {
                return Ok(None);
            }
            if admit_or_decline(offset_builder.push_end(total, vertex_cap))?.is_none() {
                return Ok(None);
            }
        }
        let Some(axes) = axes else {
            return Ok(None);
        };
        let mut builder = CoordSeqBuilder::with_capacity(axes, total);
        for shape in replay {
            let Shape::LineString(seq) = shape else {
                unreachable!("validated in the first pass");
            };
            builder.extend_window(seq, 0..seq.len());
        }
        let Some(coords) = admit_or_decline(builder.finish())? else {
            return Ok(None);
        };
        let Some(offsets) = admit_or_decline(offset_builder.finish(total))? else {
            return Ok(None);
        };
        Ok(Some((coords, offsets)))
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
    ) -> Result<Option<PackedPolygonColumns>> {
        if shapes.len() == 0 {
            return Ok(None);
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
                return Ok(None);
            };
            // Shared structural pack-admission (shell, ≥4 XY-closed rings,
            // uniform axes) — same predicate as streaming + packed GeoArrow.
            let Some(row_axes) = super::polygon_pack_axes(polygon) else {
                return Ok(None);
            };
            match axes {
                None => axes = Some(row_axes),
                Some(existing) if existing != row_axes => return Ok(None),
                Some(_) => {},
            }
            let mut poly_rings = 0_usize;
            for seq in polygon.rings() {
                debug_assert!(super::CheckedRingWindow::checked(seq, 0..seq.len()).is_ok());
                let Some(next) = total_vertices.checked_add(seq.len()) else {
                    return Ok(None);
                };
                total_vertices = next;
                if total_vertices > vertex_cap {
                    return Ok(None);
                }
                if admit_or_decline(ring_builder.push_end(total_vertices, vertex_cap))?.is_none() {
                    return Ok(None);
                }
                let Some(next_ring) = ring_index.checked_add(1) else {
                    return Ok(None);
                };
                ring_index = next_ring;
                if ring_index > vertex_cap {
                    return Ok(None);
                }
                let Some(next_poly_rings) = poly_rings.checked_add(1) else {
                    return Ok(None);
                };
                poly_rings = next_poly_rings;
            }
            // polygon_pack_axes already requires a nonempty ring set.
            debug_assert!(poly_rings > 0);
            if admit_or_decline(polygon_builder.push_end(ring_index, vertex_cap))?.is_none() {
                return Ok(None);
            }
        }
        let Some(axes) = axes else {
            return Ok(None);
        };
        let mut builder = CoordSeqBuilder::with_capacity(axes, total_vertices);
        for shape in replay {
            let Shape::Polygon(polygon) = shape else {
                unreachable!("validated in the first pass");
            };
            for seq in polygon.rings() {
                builder.extend_window(seq, 0..seq.len());
            }
        }
        let Some(coords) = admit_or_decline(builder.finish())? else {
            return Ok(None);
        };
        let Some(ring_offsets) = admit_or_decline(ring_builder.finish(total_vertices))? else {
            return Ok(None);
        };
        let ring_offsets = ring_offsets.cast_level::<RingLevel>();
        let Some(polygon_offsets) = admit_or_decline(polygon_builder.finish(ring_index))? else {
            return Ok(None);
        };
        let polygon_offsets = polygon_offsets.cast_level::<PolygonLevel>();
        Ok(Some((coords, ring_offsets, polygon_offsets)))
    }

    /// The shared point column when every shape is a `Point` of uniform axes,
    /// else `None`. Mixed-axis points stay `Mixed` — packing them into one
    /// `CoordSeq` would lie about per-row Z/M presence. The single packability
    /// rule behind [`from_shapes`](Self::from_shapes) and
    /// [`pack_or_mixed`](Self::pack_or_mixed).
    pub(crate) fn uniform_point_column<'a>(
        shapes: impl ExactSizeIterator<Item = &'a Shape> + Clone,
    ) -> Result<Option<CoordSeq>> {
        if shapes.len() == 0 {
            return Ok(None);
        }
        let mut axes: Option<CoordinateAxes> = None;
        for shape in shapes.clone() {
            let Shape::Point(point) = shape else {
                return Ok(None);
            };
            let point_axes = CoordinateAxes::from_point(*point);
            match axes {
                None => {
                    axes = Some(point_axes);
                },
                Some(existing) if existing != point_axes => return Ok(None),
                Some(_) => {},
            }
        }
        let Some(axes) = axes else {
            return Ok(None);
        };
        let mut builder = CoordSeqBuilder::with_capacity(axes, shapes.len());
        for shape in shapes {
            let Shape::Point(point) = shape else {
                unreachable!("validated in the first pass");
            };
            builder.push(*point);
        }
        admit_or_decline(builder.finish())
    }
}
