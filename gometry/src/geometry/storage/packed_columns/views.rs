use super::resolve::{resolve_line_columns, resolve_polygon_columns};
use super::{
    CoordSeq, CoordWindow, Cow, CsrOffsetBuilder, CsrOffsetColumn, MissingIter,
    PackedColumnBuilder, PackedRow, Point, PolygonLevel, RingLevel, RowSelectionRef, ValidityMask,
    admit_packed_total, csr_value_window, materialized_packed_lines_len,
    materialized_packed_polygons_len, storage,
};

/// Logical point rows over packed coordinates.
///
/// The cursor owns the sequential validity/rank walk used by point kernels.
/// Nullable point admission guarantees present-only physical coordinates, so
/// every present bit resolves through the selected present-rank mapping.
#[derive(Clone, Copy)]
pub(crate) struct PointRows<'a> {
    logical_len: usize,
    row_map: RowSelectionRef<'a>,
    missing: Option<&'a ValidityMask>,
}

#[derive(Clone)]
pub(crate) struct PointRowCursor<'a> {
    logical: usize,
    remaining: usize,
    row_map: RowSelectionRef<'a>,
    validity: Option<MissingIter<'a>>,
    present: usize,
}

#[derive(Clone, Copy)]
pub(crate) struct PointColumns<'a> {
    pub(super) coords: &'a CoordSeq,
    pub(super) row_map: RowSelectionRef<'a>,
    pub(super) missing: Option<&'a ValidityMask>,
}

pub(crate) struct XyColumns<'a> {
    pub xs: &'a [f64],
    pub ys: &'a [f64],
}

#[derive(Clone, Copy)]
pub(crate) struct LineColumns<'a> {
    pub(super) coords: &'a CoordSeq,
    pub(super) offsets: &'a CsrOffsetColumn,
    pub(super) row_map: RowSelectionRef<'a>,
    pub(super) missing: Option<&'a ValidityMask>,
}

#[derive(Clone, Copy)]
pub(crate) struct PolygonColumns<'a> {
    pub(super) coords: &'a CoordSeq,
    pub(super) ring_offsets: &'a CsrOffsetColumn<RingLevel>,
    pub(super) polygon_offsets: &'a CsrOffsetColumn<PolygonLevel>,
    pub(super) row_map: RowSelectionRef<'a>,
    pub(super) missing: Option<&'a ValidityMask>,
}

/// Topology-only descriptor for dual-strategy columnar reduction over packed
/// line/ring storage.
///
/// Owns **offsets and missingness only** — arithmetic stays monomorphized in
/// the domain kernel (length, perimeter, …). Leaf offsets bound contiguous
/// vertex runs (one line, one ring). Group offsets map output rows onto leaf
/// ranges (`None` = identity for lines; `Some` = polygon→ring CSR). Group-level
/// missing masks mark NaN output slots without exposing placeholder coordinates.
#[derive(Clone, Copy)]
pub(crate) struct SegmentedRuns<'a> {
    /// Leaf run boundaries in the physical vertex column (`len = n_leaves + 1`).
    pub leaf_offsets: &'a [i32],
    /// Group boundaries in leaf space (`len = n_groups + 1`). `None` means
    /// identity: group `i` owns only leaf `i`.
    pub group_offsets: Option<&'a [i32]>,
    /// Per-group missing mask (`None` = all present). Length = `n_groups`.
    pub group_missing: Option<&'a ValidityMask>,
    pub row_map: RowSelectionRef<'a>,
}

#[derive(Clone, Copy)]
pub(crate) enum PackedColumns<'a> {
    Points(PointColumns<'a>),
    Lines(LineColumns<'a>),
    Polygons(PolygonColumns<'a>),
}

impl<'a> PointRows<'a> {
    fn new(
        coords_len: usize,
        row_map: RowSelectionRef<'a>,
        missing: Option<&'a ValidityMask>,
    ) -> Self {
        let selected_len = row_map.len(coords_len);
        let logical_len = missing.map_or(selected_len, ValidityMask::len);
        Self {
            logical_len,
            row_map,
            missing,
        }
    }

    pub(crate) const fn present_count(self) -> usize {
        match self.missing {
            Some(mask) => mask.present_count(),
            None => self.logical_len,
        }
    }

    pub(crate) fn cursor(self) -> PointRowCursor<'a> {
        PointRowCursor {
            logical: 0,
            remaining: self.logical_len,
            row_map: self.row_map,
            validity: self.missing.map(ValidityMask::iter),
            present: 0,
        }
    }
}

impl Iterator for PointRowCursor<'_> {
    type Item = (usize, Option<usize>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let logical = self.logical;
        self.logical = self.logical.wrapping_add(1);
        self.remaining = self.remaining.wrapping_sub(1);
        let physical = match &mut self.validity {
            Some(validity) => {
                let missing = validity.next().expect("point validity length is exact");
                if missing {
                    None
                } else {
                    let present = self.present;
                    self.present = self.present.wrapping_add(1);
                    Some(self.row_map.physical(present))
                }
            },
            None => Some(self.row_map.physical(logical)),
        };
        Some((logical, physical))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for PointRowCursor<'_> {}

impl PointColumns<'_> {
    pub(crate) const fn coords(&self) -> &CoordSeq {
        self.coords
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "physical is yielded by PointRows; XY-only kernels avoid loading unused Z/M ordinates"
    )]
    pub(crate) fn physical_xy_at(&self, physical: usize) -> (f64, f64) {
        (self.coords.xs()[physical], self.coords.ys()[physical])
    }

    pub(crate) fn physical_xy(&self) -> XyColumns<'_> {
        let coords = self.coords();
        XyColumns {
            xs: coords.xs(),
            ys: coords.ys(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.missing
            .map_or_else(|| self.row_map.len(self.coords().len()), ValidityMask::len)
    }

    pub(crate) fn rows(&self) -> PointRows<'_> {
        PointRows::new(self.coords.len(), self.row_map, self.missing)
    }

    pub(crate) fn present_points(&self) -> impl Iterator<Item = Point> + '_ {
        let coords = self.coords;
        self.rows()
            .cursor()
            .filter_map(move |(_, physical)| physical.map(|physical| coords.point_at(physical)))
    }

    pub(crate) fn contiguous_coords(&self) -> Option<CoordSeq> {
        if self.missing.is_some() {
            return None;
        }
        match self.row_map {
            RowSelectionRef::Identity => Some(self.coords().clone()),
            RowSelectionRef::Window { start, len } => Some(self.coords.view(
                CoordWindow::from_range(start..start.wrapping_add(len), self.coords.len()),
            )),
            RowSelectionRef::Gather(_) => None,
        }
    }

    pub(crate) fn contiguous_xy(&self) -> Option<XyColumns<'_>> {
        if self.missing.is_some() {
            return None;
        }
        let range = match self.row_map {
            RowSelectionRef::Identity => 0..self.coords.len(),
            RowSelectionRef::Window { start, len } => start..start.wrapping_add(len),
            RowSelectionRef::Gather(_) => return None,
        };
        Some(XyColumns {
            xs: storage::column_window(self.coords.xs(), &range),
            ys: storage::column_window(self.coords.ys(), &range),
        })
    }

    pub(crate) fn geodesic_xy_lanes(&self) -> (Cow<'_, [f64]>, Cow<'_, [f64]>) {
        debug_assert!(
            self.missing.is_none(),
            "geodesic lanes require present rows"
        );
        if let Some(columns) = self.contiguous_xy() {
            return (Cow::Borrowed(columns.xs), Cow::Borrowed(columns.ys));
        }
        let mut xs = Vec::with_capacity(self.len());
        let mut ys = Vec::with_capacity(self.len());
        for (_, physical) in self.rows().cursor() {
            let physical = physical.expect("geodesic lanes require present rows");
            let (x, y) = self.physical_xy_at(physical);
            xs.push(x);
            ys.push(y);
        }
        (Cow::Owned(xs), Cow::Owned(ys))
    }
}

impl<'a> LineColumns<'a> {
    pub(crate) const fn selection(&self) -> RowSelectionRef<'a> {
        self.row_map
    }

    pub(crate) const fn coords(&self) -> &CoordSeq {
        self.coords
    }

    pub(crate) fn offsets(&self) -> &[i32] {
        self.offsets.as_slice()
    }

    pub(crate) fn physical_xy(&self) -> XyColumns<'_> {
        let coords = self.coords();
        XyColumns {
            xs: coords.xs(),
            ys: coords.ys(),
        }
    }

    pub(crate) fn rows(&self) -> usize {
        self.row_map.len(self.offsets().len().saturating_sub(1))
    }

    pub(crate) fn is_missing(&self, logical: usize) -> bool {
        self.missing.is_some_and(|mask| mask.is_missing(logical))
    }

    pub(crate) fn row_window(&self, logical: usize) -> std::ops::Range<usize> {
        self.packed_row(logical)
            .vertex_window()
            .expect("packed line row has a coordinate window")
    }

    pub(crate) fn packed_row(&self, logical: usize) -> PackedRow<'a> {
        PackedRow::line(logical, self.coords, self.offsets, self.row_map)
    }

    pub(crate) const fn from_parts(
        coords: &'a CoordSeq,
        offsets: &'a CsrOffsetColumn,
        row_map: RowSelectionRef<'a>,
    ) -> Self {
        resolve_line_columns(coords, offsets, row_map, None)
    }

    pub(crate) fn materialized_vertex_len(&self) -> crate::error::Result<usize> {
        materialized_packed_lines_len(self.offsets(), self.row_map)
    }

    pub(crate) fn materialized_present_vertices(
        &self,
        minimum_row_len: usize,
    ) -> crate::error::Result<usize> {
        let mut vertices = 0;
        for row in 0..self.rows() {
            if self.is_missing(row) {
                continue;
            }
            let len = self.row_window(row).len();
            if len >= minimum_row_len {
                vertices = admit_packed_total(vertices, len)?;
            }
        }
        Ok(vertices)
    }

    /// Reverse each selected CSR window into identity columns. Identity
    /// reverse keeps sharing the source offset table via
    /// [`crate::geometry::storage::reverse_coord_windows`] instead.
    pub(crate) fn reverse_selected(&self) -> crate::error::Result<(CoordSeq, CsrOffsetColumn)> {
        let vertices = self.materialized_vertex_len()?;
        let mut builder = PackedColumnBuilder::like(self.coords, vertices, self.rows());
        for logical in 0..self.rows() {
            builder.push_reversed_window_admitted(self.coords, self.row_window(logical))?;
        }
        let cap = builder.vertex_len();
        builder.finish(cap)
    }
}

impl SegmentedRuns<'_> {
    pub(crate) fn n_groups(&self) -> usize {
        let physical = self.group_offsets.map_or_else(
            || self.n_leaves(),
            |offsets| offsets.len().saturating_sub(1),
        );
        self.row_map.len(physical)
    }

    pub(crate) const fn n_leaves(&self) -> usize {
        self.leaf_offsets.len().saturating_sub(1)
    }

    /// Leaf-index range owned by `group`.
    pub(crate) fn group_leaves(&self, group: usize) -> std::ops::Range<usize> {
        let group = self.row_map.physical(group);
        self.group_offsets.map_or_else(
            || group..group.wrapping_add(1),
            |offsets| RowSelectionRef::Identity.csr_window(offsets, group),
        )
    }

    /// Vertex window for leaf `leaf` in the physical column.
    pub(crate) const fn leaf_vertices(&self, leaf: usize) -> std::ops::Range<usize> {
        RowSelectionRef::Identity.csr_window(self.leaf_offsets, leaf)
    }

    /// True when the group is marked missing.
    pub(crate) fn is_missing(&self, group: usize) -> bool {
        self.group_missing
            .is_some_and(|mask| mask.is_missing(group))
    }
}

impl LineColumns<'_> {
    /// Topology descriptor for planar line-length dual reduction. Group space
    /// is identity over leaves (one line row = one leaf).
    pub(crate) fn segmented_runs<'a>(
        &'a self,
        missing: Option<&'a ValidityMask>,
    ) -> SegmentedRuns<'a> {
        SegmentedRuns {
            leaf_offsets: self.offsets(),
            group_offsets: None,
            group_missing: missing,
            row_map: self.row_map,
        }
    }
}

impl PolygonColumns<'_> {
    /// Topology descriptor for planar polygon perimeter dual reduction.
    pub(crate) fn segmented_runs<'a>(
        &'a self,
        missing: Option<&'a ValidityMask>,
    ) -> SegmentedRuns<'a> {
        SegmentedRuns {
            leaf_offsets: self.ring_offsets(),
            group_offsets: Some(self.polygon_offsets()),
            group_missing: missing,
            row_map: self.row_map,
        }
    }
}

impl<'a> PolygonColumns<'a> {
    pub(crate) const fn coords(&self) -> &CoordSeq {
        self.coords
    }

    pub(crate) fn ring_offsets(&self) -> &[i32] {
        self.ring_offsets.as_slice()
    }

    pub(crate) fn polygon_offsets(&self) -> &[i32] {
        self.polygon_offsets.as_slice()
    }

    pub(crate) fn physical_xy(&self) -> XyColumns<'_> {
        let coords = self.coords();
        XyColumns {
            xs: coords.xs(),
            ys: coords.ys(),
        }
    }

    pub(crate) fn rows(&self) -> usize {
        self.row_map
            .len(self.polygon_offsets().len().saturating_sub(1))
    }

    pub(crate) fn is_missing(&self, logical: usize) -> bool {
        self.missing.is_some_and(|mask| mask.is_missing(logical))
    }

    pub(crate) fn polygon_rings(&self, logical: usize) -> std::ops::Range<usize> {
        self.row_map.csr_window(self.polygon_offsets(), logical)
    }

    pub(crate) fn packed_row(&self, logical: usize) -> PackedRow<'a> {
        PackedRow::polygon(
            logical,
            self.coords,
            self.ring_offsets,
            self.polygon_offsets,
            self.row_map,
        )
    }

    pub(crate) const fn from_parts(
        coords: &'a CoordSeq,
        ring_offsets: &'a CsrOffsetColumn<RingLevel>,
        polygon_offsets: &'a CsrOffsetColumn<PolygonLevel>,
        row_map: RowSelectionRef<'a>,
    ) -> Self {
        resolve_polygon_columns(coords, ring_offsets, polygon_offsets, row_map, None)
    }

    pub(crate) fn materialized_sizes(&self) -> crate::error::Result<(usize, usize)> {
        materialized_packed_polygons_len(self.ring_offsets(), self.polygon_offsets(), self.row_map)
    }

    pub(crate) fn materialized_present_sizes(&self) -> crate::error::Result<(usize, usize)> {
        let mut vertices = 0;
        let mut rings = 0;
        for row in 0..self.rows() {
            if self.is_missing(row) {
                continue;
            }
            let row_rings = self.polygon_rings(row);
            if !row_rings.is_empty() {
                let offsets = self.ring_offsets();
                vertices = admit_packed_total(
                    vertices,
                    csr_value_window(offsets, row_rings.clone()).len(),
                )?;
            }
            rings = admit_packed_total(rings, row_rings.len())?;
        }
        Ok((vertices, rings))
    }

    pub(crate) fn reverse_selected(
        &self,
    ) -> crate::error::Result<(
        CoordSeq,
        CsrOffsetColumn<RingLevel>,
        CsrOffsetColumn<PolygonLevel>,
    )> {
        let (vertices, rings) = self.materialized_sizes()?;
        let mut builder = PackedColumnBuilder::like(self.coords, vertices, rings);
        let mut polygon_builder = CsrOffsetBuilder::with_capacity(self.rows());
        let mut ring_index = 0_usize;
        for logical in 0..self.rows() {
            let row = self.packed_row(logical);
            let row_rings = row
                .polygon_rings()
                .expect("packed polygon row has a ring range");
            for ring in row_rings.clone() {
                let window = row
                    .polygon_ring_window(ring)
                    .expect("ring lies in its admitted polygon row");
                builder.push_reversed_window_admitted(self.coords, window)?;
            }
            ring_index = ring_index.wrapping_add(row_rings.len());
            polygon_builder.push_end(ring_index, ring_index)?;
        }
        let cap = builder.vertex_len();
        let (out_coords, out_ring_offsets) = builder.finish(cap)?;
        let out_ring_offsets = out_ring_offsets.cast_level::<RingLevel>();
        let out_polygon_offsets = polygon_builder
            .finish(out_ring_offsets.len().saturating_sub(1))?
            .cast_level::<PolygonLevel>();
        Ok((out_coords, out_ring_offsets, out_polygon_offsets))
    }
}

impl PackedColumns<'_> {
    pub(crate) fn physical_xy(&self) -> XyColumns<'_> {
        match self {
            Self::Points(columns) => columns.physical_xy(),
            Self::Lines(columns) => columns.physical_xy(),
            Self::Polygons(columns) => columns.physical_xy(),
        }
    }

    pub(crate) fn row_count(&self) -> usize {
        match self {
            Self::Points(columns) => columns.len(),
            Self::Lines(columns) => columns.rows(),
            Self::Polygons(columns) => columns.rows(),
        }
    }

    pub(crate) fn collect_rows<T, C: FromIterator<T>>(
        &self,
        mut f: impl FnMut(Option<PackedRow<'_>>) -> T,
    ) -> C {
        match self {
            Self::Points(columns) => columns
                .rows()
                .cursor()
                .map(|(logical, physical)| {
                    f(physical
                        .map(|physical| PackedRow::point_at(logical, columns.coords, physical)))
                })
                .collect(),
            Self::Lines(columns) => (0..columns.rows())
                .map(|logical| {
                    f((!columns.is_missing(logical)).then(|| columns.packed_row(logical)))
                })
                .collect(),
            Self::Polygons(columns) => (0..columns.rows())
                .map(|logical| {
                    f((!columns.is_missing(logical)).then(|| columns.packed_row(logical)))
                })
                .collect(),
        }
    }

    /// Visit present logical rows in display order without materializing their
    /// geometry shapes. The row index remains the logical row even when point
    /// storage is compact or selected through a gather.
    pub(crate) fn try_for_each_present_row<E>(
        &self,
        mut visit: impl FnMut(usize, PackedRow<'_>) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        match self {
            Self::Points(columns) => {
                for (logical, physical) in columns.rows().cursor() {
                    if let Some(physical) = physical {
                        visit(
                            logical,
                            PackedRow::point_at(logical, columns.coords, physical),
                        )?;
                    }
                }
            },
            Self::Lines(columns) => {
                for logical in 0..columns.rows() {
                    if !columns.is_missing(logical) {
                        visit(logical, columns.packed_row(logical))?;
                    }
                }
            },
            Self::Polygons(columns) => {
                for logical in 0..columns.rows() {
                    if !columns.is_missing(logical) {
                        visit(logical, columns.packed_row(logical))?;
                    }
                }
            },
        }
        Ok(())
    }
}
