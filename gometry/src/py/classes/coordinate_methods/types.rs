use super::*;

impl PyCoordinates {
    pub const fn new(view: coordinates::CoordinateView) -> Self {
        Self { view, layout: None }
    }

    pub(crate) fn coordinates_equal(&self, other: &Self) -> bool {
        if self.view.len() != other.view.len() {
            return false;
        }
        let mut other_values = Vec::with_capacity(other.view.len());
        other.view.for_each_point(|coord| {
            other_values.push(visible_coordinate(coord.point, other.layout));
        });
        let mut idx = 0_usize;
        let mut equal = true;
        self.view.for_each_point(|coord| {
            if equal {
                equal =
                    other_values.get(idx) == Some(&visible_coordinate(coord.point, self.layout));
                idx += 1;
            }
        });
        equal
    }

    pub(crate) fn coordinates_equal_sequence(&self, other: &Bound<'_, PyAny>) -> PyResult<bool> {
        let py = other.py();
        let mut items = other.try_iter()?;
        let mut points = Vec::with_capacity(self.view.len());
        self.view.for_each_point(|coord| points.push(coord.point));
        for point in points {
            match items.next() {
                Some(Ok(item)) => {
                    if !coordinate_tuple(py, point, self.layout)?
                        .bind(py)
                        .eq(&item)?
                    {
                        return Ok(false);
                    }
                },
                _ => return Ok(false),
            }
        }
        Ok(items.next().is_none())
    }

    /// Format one coordinate as a Python tuple, honoring a fixed `select`
    /// layout (absent Z/M become `None`) or the coordinate's native axes.
    pub(crate) fn tuple(&self, py: Python<'_>, point: Point) -> PyResult<Py<PyAny>> {
        coordinate_tuple(py, point, self.layout)
    }
}

fn visible_coordinate(point: Point, layout: Option<CoordinateAxes>) -> ([Option<f64>; 4], usize) {
    let (order, n) =
        coordinate_axis_order(layout.unwrap_or_else(|| CoordinateAxes::from_point(point)));
    let mut values = [None; 4];
    for (index, &axis) in order[..n].iter().enumerate() {
        values[index] = coordinate_ordinate(point, axis);
    }
    (values, n)
}
