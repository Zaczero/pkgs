use super::*;

impl HeapSize for PyCellArray {
    fn heap_bytes(&self) -> usize {
        self.logical_heap_bytes_range(0..self.len())
            .saturating_add(self.selection.heap_bytes())
    }
}

impl RowContainer for PyCellArray {
    const LABEL: &'static str = "CellArray";
    const INDEX_LABEL: &'static str = "cell array";

    fn row_count(&self) -> usize {
        self.len()
    }

    fn scalar_row(&self, py: Python<'_>, row: usize) -> PyResult<Py<PyAny>> {
        self.cell_at(py, row)
    }
}

impl RowGetItemContainer for PyCellArray {
    fn gather_rows(&self, rows: &[usize]) -> PyResult<Self> {
        Ok(self.select_logical_rows(rows.iter().copied()))
    }

    fn slice_rows(&self, start: isize, stop: isize, step: isize, count: usize) -> PyResult<Self> {
        Ok(self.slice(start, stop, step, count))
    }

    fn empty(&self) -> PyResult<Self> {
        Ok(Self {
            storage: CellStorage::new(self.kind(), Vec::new(), None),
            selection: RowSelection::Identity,
        })
    }

    fn container_to_py(py: Python<'_>, value: Self) -> PyResult<Py<PyAny>> {
        Ok(value.into_pyobject(py)?.into_any().unbind())
    }
}
