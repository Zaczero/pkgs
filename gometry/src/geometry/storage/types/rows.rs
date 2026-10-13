use super::*;

/// Expand selected line-row construction into hot per-row callers. The row
/// selector and CSR offsets are admitted, so the fast path stores their range
/// directly instead of rebuilding a checked coordinate window.
macro_rules! packed_line_row {
    ($logical:expr, $coords:expr, $offsets:expr, $row_map:expr $(,)?) => {{
        let __logical = $logical;
        let __coords = $coords;
        let __vertices = $row_map.csr_window($offsets, __logical);
        $crate::geometry::storage::PackedRow::Lines {
            logical: __logical,
            coords: __coords,
            vertices_start: __vertices.start,
            vertices_end: __vertices.end,
        }
    }};
}

/// Expand admitted ring endpoint reads in the hot consumers instead of
/// rebuilding a range through a helper call. Callers handle zero-ring rows.
macro_rules! packed_polygon_vertex_window {
    ($ring_offsets:expr, $rings:expr $(,)?) => {{
        let __ring_offsets = $ring_offsets;
        let __rings = $rings;
        #[expect(
            clippy::indexing_slicing,
            reason = "the ring range is admitted by the selected polygon CSR row"
        )]
        let __vertices_start = __ring_offsets[__rings.start] as usize;
        #[expect(
            clippy::indexing_slicing,
            reason = "the ring range is admitted by the selected polygon CSR row"
        )]
        let __vertices_end = __ring_offsets[__rings.end] as usize;
        __vertices_start..__vertices_end
    }};
}

/// Expand planar bounds where polygon storage and row selection are known,
/// avoiding the extra `PackedRow` bounds/window accessors in the row loop.
macro_rules! packed_polygon_bounds {
    ($coords:expr, $ring_offsets:expr, $rings_start:expr, $rings_end:expr $(,)?) => {{
        let __coords = $coords;
        let __rings_start = $rings_start;
        let __rings_end = $rings_end;
        if __rings_start < __rings_end {
            let __vertices = $crate::geometry::storage::packed_polygon_vertex_window!(
                $ring_offsets,
                __rings_start..__rings_end,
            );
            $crate::geometry::storage::columns::row_bounds(
                $crate::geometry::storage::columns::column_window(__coords.xs(), &__vertices),
                $crate::geometry::storage::columns::column_window(__coords.ys(), &__vertices),
            )
        } else {
            None
        }
    }};
}

/// Expand a packed polygon's shell and holes at their row consumer so
/// validation does not cross the `PackedRow::to_polygon` helper boundary.
/// `?` propagates the first ring error in shell-then-hole order; zero rings
/// produce `None`.
macro_rules! packed_polygon_view {
    ($coords:expr, $ring_offsets:expr, $rings:expr $(,)?) => {{
        let __coords = $coords;
        let __ring_offsets = $ring_offsets;
        let __rings = $rings;
        if __rings.is_empty() {
            Ok::<Option<$crate::geometry::Polygon>, $crate::error::Error>(None)
        } else {
            let __shell_vertices =
                $crate::geometry::storage::admitted_csr_window!(__ring_offsets, __rings.start);
            let __shell = $crate::geometry::Ring::try_from_closed(__coords.view(
                $crate::geometry::CoordWindow::from_range(__shell_vertices, __coords.len()),
            ))?;
            let mut __holes = Vec::with_capacity(__rings.len().saturating_sub(1));
            for __ring in __rings.start.wrapping_add(1)..__rings.end {
                let __vertices =
                    $crate::geometry::storage::admitted_csr_window!(__ring_offsets, __ring);
                __holes.push($crate::geometry::Ring::try_from_closed(__coords.view(
                    $crate::geometry::CoordWindow::from_range(__vertices, __coords.len()),
                ))?);
            }
            Ok(Some($crate::geometry::Polygon::new(__shell, __holes)))
        }
    }};
}

/// One present packed row after selector and outer-CSR resolution. Logical
/// identity stays with the row; nullable absence remains outside this payload
/// view. Polygon coordinate windows resolve from the retained ring range only
/// when a consumer requests them.
#[derive(Clone, Copy)]
pub(crate) enum PackedRow<'a> {
    Points {
        logical: usize,
        coords: &'a CoordSeq,
        physical: usize,
    },
    Lines {
        logical: usize,
        coords: &'a CoordSeq,
        vertices_start: usize,
        vertices_end: usize,
    },
    Polygons {
        logical: usize,
        coords: &'a CoordSeq,
        ring_offsets: &'a CsrOffsetColumn<RingLevel>,
        rings_start: usize,
        rings_end: usize,
    },
}

impl<'a> PackedRow<'a> {
    pub(crate) const fn point_at(logical: usize, coords: &'a CoordSeq, physical: usize) -> Self {
        Self::Points {
            logical,
            coords,
            physical,
        }
    }

    pub(crate) const fn selected_point(
        logical: usize,
        coords: &'a CoordSeq,
        row_map: RowSelectionRef<'_>,
    ) -> Self {
        Self::point_at(logical, coords, row_map.physical(logical))
    }

    pub(crate) fn line(
        logical: usize,
        coords: &'a CoordSeq,
        offsets: &CsrOffsetColumn,
        row_map: RowSelectionRef<'_>,
    ) -> Self {
        packed_line_row!(logical, coords, offsets.as_slice(), row_map)
    }

    pub(crate) fn polygon(
        logical: usize,
        coords: &'a CoordSeq,
        ring_offsets: &'a CsrOffsetColumn<RingLevel>,
        polygon_offsets: &CsrOffsetColumn<PolygonLevel>,
        row_map: RowSelectionRef<'_>,
    ) -> Self {
        let rings = row_map.csr_window(polygon_offsets.as_slice(), logical);
        Self::polygon_window(logical, coords, ring_offsets, rings)
    }

    pub(crate) const fn polygon_window(
        logical: usize,
        coords: &'a CoordSeq,
        ring_offsets: &'a CsrOffsetColumn<RingLevel>,
        rings: std::ops::Range<usize>,
    ) -> Self {
        Self::Polygons {
            logical,
            coords,
            ring_offsets,
            rings_start: rings.start,
            rings_end: rings.end,
        }
    }

    pub(crate) fn from_storage(storage: &'a PackedStorage, logical: usize) -> Self {
        match storage {
            PackedStorage::Points { coords, row_map } => {
                Self::selected_point(logical, coords, row_map.as_deref())
            },
            PackedStorage::Lines {
                coords,
                offsets,
                row_map,
            } => Self::line(logical, coords, offsets, row_map.as_deref()),
            PackedStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            } => Self::polygon(
                logical,
                coords,
                ring_offsets,
                polygon_offsets,
                row_map.as_deref(),
            ),
        }
    }

    pub(crate) const fn index(&self) -> usize {
        match self {
            Self::Points { logical, .. }
            | Self::Lines { logical, .. }
            | Self::Polygons { logical, .. } => *logical,
        }
    }

    pub(crate) const fn kind(&self) -> GeometryKind {
        match self {
            Self::Points { .. } => GeometryKind::Point,
            Self::Lines { .. } => GeometryKind::LineString,
            Self::Polygons { .. } => GeometryKind::Polygon,
        }
    }

    pub(crate) const fn axes(&self) -> crate::geometry::CoordinateAxes {
        match self {
            Self::Points { coords, .. }
            | Self::Lines { coords, .. }
            | Self::Polygons { coords, .. } => coords.axes(),
        }
    }

    pub(crate) fn point_value(&self) -> Option<Point> {
        match self {
            Self::Points {
                coords, physical, ..
            } => Some(coords.point_at(*physical)),
            Self::Lines { .. } | Self::Polygons { .. } => None,
        }
    }

    pub(crate) fn coordinate_window(&self) -> Option<CoordWindow> {
        match self {
            Self::Points {
                physical, coords, ..
            } => Some(CoordWindow::from_range(
                *physical..physical.wrapping_add(1),
                coords.len(),
            )),
            Self::Lines {
                coords,
                vertices_start,
                vertices_end,
                ..
            } => Some(CoordWindow::from_range(
                *vertices_start..*vertices_end,
                coords.len(),
            )),
            Self::Polygons {
                coords,
                ring_offsets,
                rings_start,
                rings_end,
                ..
            } if rings_start < rings_end => Some(CoordWindow::from_range(
                packed_polygon_vertex_window!(ring_offsets.as_slice(), *rings_start..*rings_end,),
                coords.len(),
            )),
            Self::Polygons { .. } => None,
        }
    }

    pub(crate) fn vertex_window(&self) -> Option<std::ops::Range<usize>> {
        match self {
            Self::Points { physical, .. } => Some(*physical..physical.wrapping_add(1)),
            Self::Lines {
                vertices_start,
                vertices_end,
                ..
            } => Some(*vertices_start..*vertices_end),
            Self::Polygons {
                ring_offsets,
                rings_start,
                rings_end,
                ..
            } if rings_start < rings_end => Some(packed_polygon_vertex_window!(
                ring_offsets.as_slice(),
                *rings_start..*rings_end,
            )),
            Self::Polygons { .. } => None,
        }
    }

    pub(crate) const fn polygon_rings(&self) -> Option<std::ops::Range<usize>> {
        match self {
            Self::Polygons {
                rings_start,
                rings_end,
                ..
            } => Some(*rings_start..*rings_end),
            Self::Points { .. } | Self::Lines { .. } => None,
        }
    }

    pub(crate) fn polygon_ring_window(&self, ring: usize) -> Option<std::ops::Range<usize>> {
        match self {
            Self::Polygons {
                ring_offsets,
                rings_start,
                rings_end,
                ..
            } if ring >= *rings_start && ring < *rings_end => {
                Some(admitted_csr_window!(ring_offsets.as_slice(), ring))
            },
            Self::Points { .. } | Self::Lines { .. } | Self::Polygons { .. } => None,
        }
    }

    pub(crate) fn coord_count(&self) -> usize {
        match self {
            Self::Points { .. } => 1,
            Self::Lines {
                vertices_start,
                vertices_end,
                ..
            } => vertices_end.wrapping_sub(*vertices_start),
            Self::Polygons { .. } => self.vertex_window().map_or(0, |window| window.len()),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        match self {
            Self::Points { .. } => false,
            Self::Lines {
                vertices_start,
                vertices_end,
                ..
            } => vertices_start == vertices_end,
            Self::Polygons {
                rings_start,
                rings_end,
                ..
            } => rings_start == rings_end,
        }
    }

    pub(crate) fn quick_bounds(&self) -> Option<Bounds> {
        match self {
            Self::Points {
                coords, physical, ..
            } => {
                let point = coords.point_at(*physical);
                (point.x.is_finite() && point.y.is_finite()).then_some(Bounds::from_point(point))
            },
            Self::Lines {
                coords,
                vertices_start,
                vertices_end,
                ..
            } => {
                let range = *vertices_start..*vertices_end;
                row_bounds(
                    column_window(coords.xs(), &range),
                    column_window(coords.ys(), &range),
                )
            },
            Self::Polygons {
                coords,
                ring_offsets,
                rings_start,
                rings_end,
                ..
            } => packed_polygon_bounds!(coords, ring_offsets.as_slice(), *rings_start, *rings_end,),
        }
    }

    pub(crate) fn to_polygon(self) -> crate::error::Result<Option<Polygon>> {
        let Self::Polygons {
            coords,
            ring_offsets,
            rings_start,
            rings_end,
            ..
        } = self
        else {
            unreachable!("polygon materialization requires packed polygon storage")
        };
        packed_polygon_view!(coords, ring_offsets.as_slice(), rings_start..rings_end,)
    }

    pub(crate) fn to_shape(self) -> crate::error::Result<Shape> {
        match self {
            Self::Points {
                coords, physical, ..
            } => Ok(Shape::Point(coords.point_at(physical))),
            Self::Lines {
                coords,
                vertices_start,
                vertices_end,
                ..
            } => {
                if vertices_start == vertices_end {
                    Ok(Shape::typed_empty(GeometryKind::LineString, coords.axes()))
                } else {
                    Ok(Shape::LineString(LineSeq::row_view(
                        coords,
                        CoordWindow::from_range(vertices_start..vertices_end, coords.len()),
                    )))
                }
            },
            Self::Polygons { coords, .. } => Ok(self.to_polygon()?.map_or_else(
                || Shape::typed_empty(GeometryKind::Polygon, coords.axes()),
                Shape::Polygon,
            )),
        }
    }
}

pub(crate) struct PointRows<'a>(PointRowSource<'a>);

enum PointRowSource<'a> {
    Packed {
        coords: &'a CoordSeq,
        row_map: RowSelectionRef<'a>,
    },
    Mixed(&'a [Shape]),
}

impl PointRows<'_> {
    pub(crate) fn len(&self) -> usize {
        match &self.0 {
            PointRowSource::Packed { coords, row_map } => point_logical_len(coords, *row_map),
            PointRowSource::Mixed(points) => points.len(),
        }
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn get(&self, index: usize) -> Point {
        match &self.0 {
            PointRowSource::Packed { coords, row_map } => {
                coords.point_at(physical_row(*row_map, index))
            },
            PointRowSource::Mixed(points) => {
                let Shape::Point(point) = points
                    .get(index)
                    .expect("point row index is bounded by its admitted mixed slice")
                else {
                    unreachable!("point row admission excludes other shapes");
                };
                *point
            },
        }
    }

    pub(crate) fn iter(&self) -> impl ExactSizeIterator<Item = Point> + '_ {
        let len = self.len();
        (0..len).map(move |index| self.get(index))
    }
}

pub(crate) struct LineRows<'a>(LineRowSource<'a>);

enum LineRowSource<'a> {
    Packed {
        coords: &'a CoordSeq,
        offsets: &'a CsrOffsetColumn,
        row_map: RowSelectionRef<'a>,
    },
    Mixed(&'a [Shape]),
}

impl GeometryArrayStorage {
    pub(crate) fn point_rows(&self) -> Option<PointRows<'_>> {
        match self {
            Self::Mixed(shapes) => shapes
                .iter()
                .all(|shape| matches!(shape, Shape::Point(_)))
                .then_some(PointRows(PointRowSource::Mixed(shapes))),
            Self::Packed(PackedStorage::Points { coords, row_map }) => {
                Some(PointRows(PointRowSource::Packed {
                    coords,
                    row_map: row_map.as_deref(),
                }))
            },
            Self::Packed(PackedStorage::Lines { .. } | PackedStorage::Polygons { .. }) => None,
        }
    }

    pub(crate) fn line_rows(&self) -> Option<LineRows<'_>> {
        match self {
            Self::Mixed(shapes) => shapes
                .iter()
                .all(|shape| matches!(shape, Shape::LineString(_)))
                .then_some(LineRows(LineRowSource::Mixed(shapes))),
            Self::Packed(PackedStorage::Lines {
                coords,
                offsets,
                row_map,
            }) => Some(LineRows(LineRowSource::Packed {
                coords,
                offsets,
                row_map: row_map.as_deref(),
            })),
            Self::Packed(PackedStorage::Points { .. } | PackedStorage::Polygons { .. }) => None,
        }
    }
}

impl LineRows<'_> {
    pub(crate) fn len(&self) -> usize {
        match &self.0 {
            LineRowSource::Packed {
                offsets, row_map, ..
            } => line_logical_len(offsets, *row_map),
            LineRowSource::Mixed(lines) => lines.len(),
        }
    }

    pub(crate) const fn is_packed_pair(&self, other: &Self) -> bool {
        matches!(
            (&self.0, &other.0),
            (LineRowSource::Packed { .. }, LineRowSource::Packed { .. })
        )
    }

    pub(crate) fn row_xy(&self, index: usize) -> (&[f64], &[f64]) {
        match &self.0 {
            LineRowSource::Packed {
                coords,
                offsets,
                row_map,
            } => {
                let window = PackedRow::line(index, coords, offsets, *row_map)
                    .vertex_window()
                    .expect("packed line row has a coordinate window");
                (
                    column_window(coords.xs(), &window),
                    column_window(coords.ys(), &window),
                )
            },
            LineRowSource::Mixed(lines) => {
                let Shape::LineString(seq) = lines
                    .get(index)
                    .expect("line row index is bounded by its admitted mixed slice")
                else {
                    unreachable!("line row admission excludes other shapes");
                };
                (seq.xs(), seq.ys())
            },
        }
    }

    pub(crate) fn packed_column_view(&self) -> Option<crate::geometry::PackedLineColumnView<'_>> {
        match &self.0 {
            LineRowSource::Packed {
                coords,
                offsets,
                row_map,
            } => Some(crate::geometry::PackedLineColumnView {
                coords,
                offsets: offsets.as_slice(),
                row_map: *row_map,
            }),
            LineRowSource::Mixed(_) => None,
        }
    }
}
