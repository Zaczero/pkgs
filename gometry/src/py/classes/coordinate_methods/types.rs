use pyo3::types::PyIterator;

use crate::py::classes::coordinate_methods::{
    Bound, Point, Py, PyAny, PyAnyMethods as _, PyCoordinates, PyResult, Python, coordinate_tuple,
    coordinates,
};

impl PyCoordinates {
    pub const fn new(view: coordinates::CoordinateView, owner: Py<PyAny>) -> Self {
        Self {
            view,
            owner,
            layout: None,
        }
    }

    pub(crate) fn coordinates_equal(&self, py: Python<'_>, other: &Self) -> PyResult<bool> {
        // Full-scan equality is run-wise / columnar (never n× CSR `point_at`,
        // which is O(n log rows) on packed lines/polygons). Short-circuit
        // first-mismatch stays linear via dual sequential streams; identical
        // owners answer in O(1).
        self.view
            .try_equal_visible(py, &other.view, self.layout, other.layout)
    }

    /// Single-pass sequence equality against an already-acquired iterator.
    /// Provider errors after admission propagate unchanged.
    pub(crate) fn coordinates_equal_iter(
        &self,
        mut items: Bound<'_, PyIterator>,
    ) -> PyResult<bool> {
        use std::ops::ControlFlow;
        let py = items.py();
        let mut position = 0_usize;
        let walk = self.view.try_for_each_point(&mut |coord| {
            let current = position;
            position = position.wrapping_add(1);
            if let Err(error) = crate::check_python_signals(py, current) {
                return ControlFlow::Break(Err(error));
            }
            match items.next() {
                Some(Ok(item)) => match coordinate_tuple(py, coord, self.layout) {
                    Ok(expected) => match expected.bind(py).eq(&item) {
                        Ok(true) => ControlFlow::Continue(()),
                        Ok(false) => ControlFlow::Break(Ok(false)),
                        Err(err) => ControlFlow::Break(Err(err)),
                    },
                    Err(err) => ControlFlow::Break(Err(err)),
                },
                Some(Err(err)) => ControlFlow::Break(Err(err)),
                None => ControlFlow::Break(Ok(false)),
            }
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

    /// Format one coordinate as a Python tuple using the selected layout when set.
    pub(crate) fn tuple(&self, py: Python<'_>, point: Point) -> PyResult<Py<PyAny>> {
        coordinate_tuple(py, point, self.layout)
    }

    /// Visible-layout equality used by membership / count / index — the same
    /// representation iteration yields.
    pub(crate) fn visible_equals(
        &self,
        py: Python<'_>,
        point: Point,
        item: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        coordinate_tuple(py, point, self.layout)?.bind(py).eq(item)
    }
}
