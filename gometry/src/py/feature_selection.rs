//! One admitted row selection for feature geometry, attributes and IDs.

use pyo3::gc::{PyTraverseError, PyVisit};
use pyo3::prelude::*;
use pyo3::types::{PyString, PyTuple};

use crate::PyGeometryArray;
use crate::py::row::{
    RowContainer, RowGetItemContainer, RowIterState, array_getitem, slice_row_indices,
};
use crate::py::support::{feature_type, features_type};

struct FeatureSelection {
    geometries: PyGeometryArray,
    properties: Py<PyAny>,
    ids: Py<PyAny>,
    id_column: Option<Py<PyString>>,
}

impl FeatureSelection {
    fn scalar_with_factory(
        &self,
        py: Python<'_>,
        row: usize,
        factory: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let geometry = self.geometries.scalar_row(py, row)?;
        let properties = self.properties.bind(py).get_item(row)?;
        let id = self.ids.bind(py).get_item(row)?;
        Ok(factory.call1((geometry, properties, id))?.unbind())
    }

    fn select_rows<I>(&self, geometries: PyGeometryArray, rows: I) -> PyResult<Self>
    where
        I: ExactSizeIterator<Item = usize> + Clone,
    {
        Python::attach(|py| {
            let properties = self.properties.bind(py);
            let ids = self.ids.bind(py);
            let indices = PyTuple::new(py, rows.clone())?;
            let selected_properties = properties.call_method1("_take", (&indices,))?;
            let selected_ids = if let Ok(ids) = ids.cast_exact::<PyTuple>() {
                let selected = rows
                    .map(|index| ids.get_item(index))
                    .collect::<PyResult<Vec<_>>>()?;
                PyTuple::new(py, selected)?.into_any().unbind()
            } else {
                ids.call_method1("_take", (&indices,))?.unbind()
            };
            Ok(Self {
                geometries,
                properties: selected_properties.unbind(),
                ids: selected_ids,
                id_column: self.id_column.as_ref().map(|name| name.clone_ref(py)),
            })
        })
    }
}

impl RowContainer for FeatureSelection {
    const LABEL: &'static str = "Features";
    const INDEX_LABEL: &'static str = "feature";

    fn row_count(&self) -> usize {
        self.geometries.len()
    }

    fn scalar_row(&self, py: Python<'_>, row: usize) -> PyResult<Py<PyAny>> {
        let factory = feature_type(py)?.getattr("_from_snapshot")?;
        self.scalar_with_factory(py, row, &factory)
    }
}

#[pyclass(name = "_FeatureIterator", module = "gometry._lib", immutable_type)]
pub(crate) struct FeatureIterator {
    source: Option<FeatureSelection>,
    factory: Option<Py<PyAny>>,
    state: RowIterState,
}

#[pymethods]
impl FeatureIterator {
    const fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __length_hint__(&self) -> usize {
        self.source
            .as_ref()
            .map_or(0, |source| self.state.length_hint(source.row_count()))
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let (Some(source), Some(factory)) = (&self.source, &self.factory) else {
            return Ok(None);
        };
        self.state
            .next_row(source.row_count())
            .map(|row| source.scalar_with_factory(py, row, factory.bind(py)))
            .transpose()
    }

    #[expect(
        clippy::same_name_method,
        reason = "PyO3 requires this name for cyclic-GC traversal"
    )]
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        if let Some(source) = &self.source {
            visit.call(&source.properties)?;
            visit.call(&source.ids)?;
            visit.call(&source.id_column)?;
        }
        visit.call(&self.factory)
    }

    fn __clear__(slf: &Bound<'_, Self>) {
        let (source, factory) = {
            let mut borrowed = slf.borrow_mut();
            (borrowed.source.take(), borrowed.factory.take())
        };
        drop(source);
        drop(factory);
    }
}

#[pyfunction(name = "_iter_feature_rows")]
pub(crate) fn iter_feature_rows(
    py: Python<'_>,
    geometries: &Bound<'_, PyGeometryArray>,
    properties: &Bound<'_, PyAny>,
    ids: &Bound<'_, PyAny>,
) -> PyResult<Py<FeatureIterator>> {
    let factory = feature_type(py)?.getattr("_from_snapshot")?.unbind();
    Py::new(py, FeatureIterator {
        source: Some(FeatureSelection {
            geometries: geometries.get().clone(),
            properties: properties.clone().unbind(),
            ids: ids.clone().unbind(),
            id_column: None,
        }),
        factory: Some(factory),
        state: RowIterState::new(false),
    })
}

impl RowGetItemContainer for FeatureSelection {
    fn gather_rows(&self, rows: &[usize]) -> PyResult<Self> {
        let geometries = self.geometries.gather_logical_rows(rows)?;
        self.select_rows(geometries, rows.iter().copied())
    }

    fn slice_rows(&self, start: isize, stop: isize, step: isize, count: usize) -> PyResult<Self> {
        let rows = slice_row_indices(start, step, count);
        let geometries = if step == 1 {
            self.geometries
                .gather_logical_row_range(start as usize..stop.max(start) as usize)?
        } else {
            self.geometries.gather_logical_rows_iter(rows.clone())?
        };
        self.select_rows(geometries, rows)
    }

    fn empty(&self) -> PyResult<Self> {
        self.gather_rows(&[])
    }

    fn container_to_py(py: Python<'_>, value: Self) -> PyResult<Py<PyAny>> {
        let geometry = value.geometries.into_pyobject(py)?.into_any();
        Ok(features_type(py)?
            .getattr("_from_property_rows")?
            .call1((geometry, value.properties, value.ids, value.id_column))?
            .unbind())
    }
}

#[pyfunction(name = "_select_feature_rows")]
#[pyo3(signature = (geometries, properties, ids, id_column, index))]
pub(crate) fn select_feature_rows(
    py: Python<'_>,
    geometries: &Bound<'_, PyGeometryArray>,
    properties: &Bound<'_, PyAny>,
    ids: &Bound<'_, PyAny>,
    id_column: Option<&Bound<'_, PyString>>,
    index: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let selection = FeatureSelection {
        geometries: geometries.get().clone(),
        properties: properties.clone().unbind(),
        ids: ids.clone().unbind(),
        id_column: id_column.map(|name| name.clone().unbind()),
    };
    array_getitem(&selection, py, index)
}

#[pyfunction(name = "_materialize_feature_geometries")]
pub(crate) fn materialize_feature_geometries(
    py: Python<'_>,
    geometries: &Bound<'_, PyGeometryArray>,
) -> PyResult<PyGeometryArray> {
    let geometries = geometries.get();
    py.detach(|| geometries.materialize_rows().map_err(PyErr::from))
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<FeatureIterator>()?;
    module.add_function(wrap_pyfunction!(iter_feature_rows, module)?)?;
    module.add_function(wrap_pyfunction!(select_feature_rows, module)?)?;
    module.add_function(wrap_pyfunction!(materialize_feature_geometries, module)?)?;
    Ok(())
}
