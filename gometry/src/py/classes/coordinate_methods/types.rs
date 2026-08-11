use crate::py::classes::coordinate_methods::{
    Bound, Point, Py, PyAny, PyAnyMethods as _, PyCoordinates, PyResult, Python, coordinate_tuple,
    coordinates,
};

impl PyCoordinates {
    pub const fn new(view: coordinates::CoordinateView) -> Self {
        Self { view, layout: None }
    }

    pub(crate) fn coordinates_equal(&self, other: &Self) -> bool {
        // Full-scan equality is run-wise / columnar (never n× CSR `point_at`,
        // which is O(n log rows) on packed lines/polygons). Short-circuit
        // first-mismatch stays linear via dual sequential streams; identical
        // owners answer in O(1).
        self.view
            .equal_visible(&other.view, self.layout, other.layout)
    }

    /// Single-pass sequence equality: stream self's visible tuples against
    /// `other`'s iterator once. Provider errors propagate (never swallowed).
    pub(crate) fn coordinates_equal_sequence(&self, other: &Bound<'_, PyAny>) -> PyResult<bool> {
        use std::ops::ControlFlow;
        let py = other.py();
        let mut items = other.try_iter()?;
        let walk = self
            .view
            .try_for_each_point(&mut |coord| match items.next() {
                Some(Ok(item)) => match coordinate_tuple(py, coord.point, self.layout) {
                    Ok(expected) => match expected.bind(py).eq(&item) {
                        Ok(true) => ControlFlow::Continue(()),
                        Ok(false) => ControlFlow::Break(Ok(false)),
                        Err(err) => ControlFlow::Break(Err(err)),
                    },
                    Err(err) => ControlFlow::Break(Err(err)),
                },
                Some(Err(err)) => ControlFlow::Break(Err(err)),
                None => ControlFlow::Break(Ok(false)),
            });
        match walk {
            ControlFlow::Break(result) => result,
            ControlFlow::Continue(()) => match items.next() {
                None => Ok(true),
                Some(Ok(_)) => Ok(false),
                Some(Err(err)) => Err(err),
            },
        }
    }

    /// Format one coordinate as a Python tuple, honoring a fixed `select`
    /// layout (absent Z/M become `None`) or the coordinate's native axes.
    pub(crate) fn tuple(&self, py: Python<'_>, point: Point) -> PyResult<Py<PyAny>> {
        coordinate_tuple(py, point, self.layout)
    }

    /// Visible-layout equality used by membership / count / index — the same
    /// representation iteration yields. A `select('XY')` view matches on XY
    /// only; a forced XYZ layout matches `(x, y, None)` when Z is absent.
    pub(crate) fn visible_equals(
        &self,
        py: Python<'_>,
        point: Point,
        item: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        self.tuple(py, point)?.bind(py).eq(item)
    }
}
