use std::ops::Range;

use super::{LineColumns, PackedColumns, PointColumns, PolygonColumns};
use crate::geographic::GeographicFrame;
use crate::geometry::storage::RowSelectionRef;
use crate::geometry::storage::validity::ValidityMask;

struct OrdinaryBounds {
    longitude: f64,
    half_period: f64,
    pole: f64,
}

impl OrdinaryBounds {
    fn new(frame: GeographicFrame) -> Self {
        Self {
            longitude: frame.longitude().ordinary_longitude_bound(),
            half_period: frame.longitude().half_period(),
            pole: frame.pole(),
        }
    }

    fn coordinate(&self, x: f64, y: f64) -> bool {
        x.abs() < self.longitude && y.abs() <= self.pole
    }

    fn xy(&self, xs: &[f64], ys: &[f64]) -> bool {
        xs.iter().zip(ys).all(|(&x, &y)| self.coordinate(x, y))
    }

    fn chain(&self, xs: &[f64], ys: &[f64]) -> bool {
        let (Some((&first_x, remaining_xs)), Some((&first_y, remaining_ys))) =
            (xs.split_first(), ys.split_first())
        else {
            return xs.is_empty() && ys.is_empty();
        };
        if !self.coordinate(first_x, first_y) {
            return false;
        }
        let mut previous_x = first_x;
        for (&x, &y) in remaining_xs.iter().zip(remaining_ys) {
            if !self.coordinate(x, y) || (x - previous_x).abs() > self.half_period {
                return false;
            }
            previous_x = x;
        }
        true
    }

    fn chains(&self, xs: &[f64], ys: &[f64], offsets: &[i32]) -> bool {
        for pair in offsets.windows(2) {
            let Some(vertices) = offset_pair(pair) else {
                return false;
            };
            let (Some(run_xs), Some(run_ys)) = (xs.get(vertices.clone()), ys.get(vertices)) else {
                return false;
            };
            if !self.chain(run_xs, run_ys) {
                return false;
            }
        }
        true
    }

    fn line_columns(&self, columns: &LineColumns<'_>) -> bool {
        if let Some(rows) = dense_window(
            columns.selection(),
            columns.offsets().len().saturating_sub(1),
            columns.missing,
        ) {
            let Some(offsets) = row_offsets(columns.offsets(), rows) else {
                return false;
            };
            return self.chains(columns.coords().xs(), columns.coords().ys(), offsets);
        }
        for logical in 0..columns.rows() {
            if columns.is_missing(logical) {
                continue;
            }
            if !self.line_row(columns, logical) {
                return false;
            }
        }
        true
    }

    fn line_row(&self, columns: &LineColumns<'_>, logical: usize) -> bool {
        let vertices = columns.row_window(logical);
        let (Some(xs), Some(ys)) = (
            columns.coords().xs().get(vertices.clone()),
            columns.coords().ys().get(vertices),
        ) else {
            return false;
        };
        self.chain(xs, ys)
    }

    fn polygon_columns(&self, columns: &PolygonColumns<'_>) -> bool {
        if let Some(rows) = dense_window(
            columns.row_map,
            columns.polygon_offsets().len().saturating_sub(1),
            columns.missing,
        ) {
            let Some(polygon_offsets) = row_offsets(columns.polygon_offsets(), rows) else {
                return false;
            };
            let (Some(first_ring), Some(last_ring)) =
                (polygon_offsets.first(), polygon_offsets.last())
            else {
                return false;
            };
            let (Ok(first_ring), Ok(last_ring)) =
                (usize::try_from(*first_ring), usize::try_from(*last_ring))
            else {
                return false;
            };
            let Some(ring_offsets) = row_offsets(columns.ring_offsets(), first_ring..last_ring)
            else {
                return false;
            };
            return self.chains(columns.coords().xs(), columns.coords().ys(), ring_offsets);
        }
        for logical in 0..columns.rows() {
            if columns.is_missing(logical) {
                continue;
            }
            let Some(offsets) = row_offsets(columns.ring_offsets(), columns.polygon_rings(logical))
            else {
                return false;
            };
            if !self.chains(columns.coords().xs(), columns.coords().ys(), offsets) {
                return false;
            }
        }
        true
    }

    fn point_columns(&self, columns: &PointColumns<'_>) -> bool {
        if let Some(xy) = columns.contiguous_xy() {
            return self.xy(xy.xs, xy.ys);
        }
        for (_, physical) in columns.rows().cursor() {
            let Some(physical) = physical else {
                continue;
            };
            let (x, y) = columns.physical_xy_at(physical);
            if !self.coordinate(x, y) {
                return false;
            }
        }
        true
    }
}

impl PackedColumns<'_> {
    /// Certify frame-aware topology preflight directly from packed XY columns.
    ///
    /// Success proves that every coordinate is admitted and every line or ring
    /// edge has zero chart transit. A failed certificate leaves diagnostics and
    /// seam-fact computation to the ordered shape-row path.
    pub(crate) fn ordinary_geographic_predicates(&self, frame: GeographicFrame) -> bool {
        let bounds = OrdinaryBounds::new(frame);
        match self {
            Self::Points(columns) => bounds.point_columns(columns),
            Self::Lines(columns) => bounds.line_columns(columns),
            Self::Polygons(columns) => bounds.polygon_columns(columns),
        }
    }
}

fn dense_window(
    selection: RowSelectionRef<'_>,
    physical_rows: usize,
    missing: Option<&ValidityMask>,
) -> Option<Range<usize>> {
    if missing.is_some() {
        return None;
    }
    let range = match selection {
        RowSelectionRef::Identity => 0..physical_rows,
        RowSelectionRef::Window { start, len } => start..start.checked_add(len)?,
        RowSelectionRef::Gather(_) => return None,
    };
    (range.end <= physical_rows).then_some(range)
}

fn row_offsets(offsets: &[i32], rows: Range<usize>) -> Option<&[i32]> {
    let end = rows.end.checked_add(1)?;
    offsets.get(rows.start..end)
}

fn offset_pair(offsets: &[i32]) -> Option<Range<usize>> {
    let [start, end] = offsets else {
        return None;
    };
    let start = usize::try_from(*start).ok()?;
    let end = usize::try_from(*end).ok()?;
    Some(start..end)
}
