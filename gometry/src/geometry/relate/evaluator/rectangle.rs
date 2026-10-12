use super::*;

/// The exact relation of two certified rectangles is the product of their
/// one-dimensional endpoint/open-interval partitions.
pub(super) fn rectangle_relation(
    operands: &[Operand<'_>; 2],
    bounds: &[Option<Bounds>; 2],
) -> Option<De9im> {
    let [Some(left_bounds), Some(right_bounds)] = *bounds else {
        return None;
    };
    if !is_rectangle_matrix_operand(operands[0].row, left_bounds)
        || !is_rectangle_matrix_operand(operands[1].row, right_bounds)
    {
        return None;
    }

    let x = AxisPartition::new(
        left_bounds.minx(),
        left_bounds.maxx(),
        right_bounds.minx(),
        right_bounds.maxx(),
    );
    let y = AxisPartition::new(
        left_bounds.miny(),
        left_bounds.maxy(),
        right_bounds.miny(),
        right_bounds.maxy(),
    );
    let mut matrix = De9im::empty_disjoint();
    for x_index in 0..x.cell_count() {
        let x_cell = x.cell(x_index);
        let left_x = x_cell.location(left_bounds.minx(), left_bounds.maxx());
        let right_x = x_cell.location(right_bounds.minx(), right_bounds.maxx());
        for y_index in 0..y.cell_count() {
            let y_cell = y.cell(y_index);
            let left_y = y_cell.location(left_bounds.miny(), left_bounds.maxy());
            let right_y = y_cell.location(right_bounds.miny(), right_bounds.maxy());
            matrix.set_at_least(
                rectangle_location(left_x, left_y),
                rectangle_location(right_x, right_y),
                match x_cell.dimension().wrapping_add(y_cell.dimension()) {
                    0 => Dimension::Point,
                    1 => Dimension::Curve,
                    2 => Dimension::Surface,
                    _ => unreachable!("a planar cell has dimension at most two"),
                },
            );
        }
    }
    Some(matrix)
}

pub(super) fn is_rectangle_matrix_operand(
    row: ShapeRow<'_>,
    bounds: crate::geometry::Bounds,
) -> bool {
    let Some(mut rings) = row.polygon_rings() else {
        return false;
    };
    let Some(shell) = rings.next() else {
        return false;
    };
    // This certificate targets canonical box rows; collinear subdivisions and
    // richer polygon structures stay on the general contact evaluator.
    shell.coord_count() == 5
        && rings.next().is_none()
        && crate::geometry::is_axis_aligned_rectangle_row(row, bounds)
}

#[derive(Clone, Copy)]
pub(super) struct AxisPartition {
    pub(super) cuts: [f64; 4],
    pub(super) len: usize,
}

#[derive(Clone, Copy)]
pub(super) struct AxisCell {
    pub(super) start: Option<f64>,
    pub(super) end: Option<f64>,
    pub(super) point: bool,
}

impl AxisPartition {
    #[expect(
        clippy::indexing_slicing,
        reason = "the partition owns exactly four rectangle endpoints, so every unique cut fits its fixed array"
    )]
    #[expect(
        clippy::float_cmp,
        reason = "exact equality identifies shared represented rectangle endpoints without merging nearby cuts"
    )]
    pub(super) fn new(left_min: f64, left_max: f64, right_min: f64, right_max: f64) -> Self {
        let mut endpoints = [left_min, left_max, right_min, right_max];
        endpoints.sort_unstable_by(f64::total_cmp);
        let mut cuts = [endpoints[0]; 4];
        let mut len: usize = 1;
        for endpoint in endpoints.into_iter().skip(1) {
            if cuts[len.wrapping_sub(1)] != endpoint {
                cuts[len] = endpoint;
                len = len.wrapping_add(1);
            }
        }
        Self { cuts, len }
    }

    pub(super) const fn cell_count(self) -> usize {
        self.len.wrapping_mul(2).wrapping_add(1)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "the cell ordinal is admitted by cell_count and every endpoint is in the fixed four-cut array"
    )]
    pub(super) fn cell(self, ordinal: usize) -> AxisCell {
        if ordinal % 2 == 1 {
            let point = self.cuts[ordinal / 2];
            AxisCell {
                start: Some(point),
                end: None,
                point: true,
            }
        } else {
            let interval = ordinal / 2;
            AxisCell {
                start: interval.checked_sub(1).map(|index| self.cuts[index]),
                end: (interval < self.len).then(|| self.cuts[interval]),
                point: false,
            }
        }
    }
}

impl AxisCell {
    pub(super) fn dimension(self) -> u8 {
        u8::from(!self.point)
    }

    #[expect(
        clippy::float_cmp,
        reason = "exact endpoint equality distinguishes the rectangle boundary from its interior"
    )]
    pub(super) fn location(self, min: f64, max: f64) -> Loc {
        if self.point {
            let point = self.start.expect("point cell has one coordinate");
            if point < min || point > max {
                Loc::Exterior
            } else if point == min || point == max {
                Loc::Boundary
            } else {
                Loc::Interior
            }
        } else if self.start.is_some_and(|start| start >= min)
            && self.end.is_some_and(|end| end <= max)
        {
            Loc::Interior
        } else {
            Loc::Exterior
        }
    }
}

pub(super) fn rectangle_location(x: Loc, y: Loc) -> Loc {
    if x == Loc::Exterior || y == Loc::Exterior {
        Loc::Exterior
    } else if x == Loc::Boundary || y == Loc::Boundary {
        Loc::Boundary
    } else {
        Loc::Interior
    }
}
