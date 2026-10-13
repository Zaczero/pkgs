#[cfg(test)]
use std::cell::Cell;

use super::{
    CoordSeq, CsrOffsetColumn, GeometryArrayStorage, PackedRow, PackedStorage, PolygonLevel,
    RingLevel, RowSelectionRef, Shape, ShapeRow, line_logical_len, point_logical_len,
    polygon_logical_len,
};

#[cfg(test)]
std::thread_local! {
    static SHAPE_ITER_MATERIALIZATIONS: Cell<usize> = const { Cell::new(0) };
    static CSR_SIZE_ROW_VISITS: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_shape_iter_materializations() {
    SHAPE_ITER_MATERIALIZATIONS.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn shape_iter_materializations() -> usize {
    SHAPE_ITER_MATERIALIZATIONS.with(Cell::get)
}

#[cfg(test)]
fn record_shape_iter_materialization() {
    SHAPE_ITER_MATERIALIZATIONS.with(|count| count.set(count.get().saturating_add(1)));
}

#[cfg(test)]
pub(crate) fn reset_csr_size_row_visits() {
    CSR_SIZE_ROW_VISITS.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn csr_size_row_visits() -> usize {
    CSR_SIZE_ROW_VISITS.with(Cell::get)
}

#[cfg(test)]
pub(crate) fn record_csr_size_row_visit() {
    CSR_SIZE_ROW_VISITS.with(|count| count.set(count.get().saturating_add(1)));
}

enum PhysicalRow<'a> {
    Mixed(&'a Shape),
    Packed(PackedRow<'a>),
}

enum PhysicalRowsIter<'a> {
    Mixed {
        items: &'a [Shape],
        index: usize,
    },
    Points {
        coords: &'a CoordSeq,
        row_map: RowSelectionRef<'a>,
        index: usize,
        len: usize,
    },
    Lines {
        coords: &'a CoordSeq,
        offsets: &'a CsrOffsetColumn,
        row_map: RowSelectionRef<'a>,
        index: usize,
        len: usize,
    },
    Polygons {
        coords: &'a CoordSeq,
        ring_offsets: &'a CsrOffsetColumn<RingLevel>,
        polygon_offsets: &'a CsrOffsetColumn<PolygonLevel>,
        row_map: RowSelectionRef<'a>,
        index: usize,
        len: usize,
    },
}

impl<'a> PhysicalRowsIter<'a> {
    fn new(storage: &'a GeometryArrayStorage) -> Self {
        match storage {
            GeometryArrayStorage::Mixed(items) => Self::Mixed { items, index: 0 },
            GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) => {
                let row_map = row_map.as_deref();
                Self::Points {
                    coords,
                    row_map,
                    index: 0,
                    len: point_logical_len(coords, row_map),
                }
            },
            GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords,
                offsets,
                row_map,
            }) => {
                let row_map = row_map.as_deref();
                Self::Lines {
                    coords,
                    offsets,
                    row_map,
                    index: 0,
                    len: line_logical_len(offsets.as_slice(), row_map),
                }
            },
            GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            }) => {
                let row_map = row_map.as_deref();
                Self::Polygons {
                    coords,
                    ring_offsets,
                    polygon_offsets,
                    row_map,
                    index: 0,
                    len: polygon_logical_len(polygon_offsets.as_slice(), row_map),
                }
            },
        }
    }
}

impl<'a> Iterator for PhysicalRowsIter<'a> {
    type Item = PhysicalRow<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Mixed { items, index } => {
                if *index >= items.len() {
                    return None;
                }
                let item = PhysicalRow::Mixed(
                    items
                        .get(*index)
                        .expect("mixed row index is bounded by its admitted slice"),
                );
                *index = (*index).wrapping_add(1);
                Some(item)
            },
            Self::Points {
                coords,
                row_map,
                index,
                len,
            } => {
                if *index >= *len {
                    return None;
                }
                let row = PackedRow::selected_point(*index, coords, *row_map);
                *index = (*index).wrapping_add(1);
                Some(PhysicalRow::Packed(row))
            },
            Self::Lines {
                coords,
                offsets,
                row_map,
                index,
                len,
            } => {
                if *index >= *len {
                    return None;
                }
                let row = PackedRow::line(*index, coords, offsets, *row_map);
                *index = (*index).wrapping_add(1);
                Some(PhysicalRow::Packed(row))
            },
            Self::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
                index,
                len,
            } => {
                if *index >= *len {
                    return None;
                }
                let row =
                    PackedRow::polygon(*index, coords, ring_offsets, polygon_offsets, *row_map);
                *index = (*index).wrapping_add(1);
                Some(PhysicalRow::Packed(row))
            },
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = match self {
            Self::Mixed { items, index } => items.len().saturating_sub(*index),
            Self::Points { index, len, .. }
            | Self::Lines { index, len, .. }
            | Self::Polygons { index, len, .. } => len.saturating_sub(*index),
        };
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for PhysicalRowsIter<'_> {
    fn len(&self) -> usize {
        self.size_hint().0
    }
}

pub(crate) struct ShapesIter<'a> {
    rows: PhysicalRowsIter<'a>,
}

impl<'a> ShapesIter<'a> {
    pub(crate) fn new(storage: &'a GeometryArrayStorage) -> Self {
        Self {
            rows: PhysicalRowsIter::new(storage),
        }
    }
}

impl<'a> Iterator for ShapesIter<'a> {
    type Item = crate::error::Result<std::borrow::Cow<'a, Shape>>;

    fn next(&mut self) -> Option<Self::Item> {
        let row = self.rows.next()?;
        #[cfg(test)]
        record_shape_iter_materialization();
        match row {
            PhysicalRow::Mixed(shape) => Some(Ok(std::borrow::Cow::Borrowed(shape))),
            PhysicalRow::Packed(row) => Some(row.to_shape().map(std::borrow::Cow::Owned)),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.rows.size_hint()
    }
}

impl ExactSizeIterator for ShapesIter<'_> {
    fn len(&self) -> usize {
        self.size_hint().0
    }
}

pub(crate) struct RowsIter<'a> {
    rows: PhysicalRowsIter<'a>,
}

impl<'a> RowsIter<'a> {
    pub(crate) fn new(storage: &'a GeometryArrayStorage) -> Self {
        Self {
            rows: PhysicalRowsIter::new(storage),
        }
    }
}

impl<'a> Iterator for RowsIter<'a> {
    type Item = ShapeRow<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.rows.next()? {
            PhysicalRow::Mixed(shape) => Some(ShapeRow::Shape(shape)),
            PhysicalRow::Packed(row) => Some(ShapeRow::from_packed(row)),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.rows.size_hint()
    }
}

impl ExactSizeIterator for RowsIter<'_> {
    fn len(&self) -> usize {
        self.size_hint().0
    }
}
