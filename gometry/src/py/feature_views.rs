//! Lazy read-only views over owned feature property snapshots.

use std::sync::Arc;

use pyo3::basic::CompareOp;
use pyo3::exceptions::{PyIndexError, PyKeyError, PyRuntimeError};
use pyo3::gc::{PyTraverseError, PyVisit};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PySlice, PyString, PyTuple};

use crate::PyGeometryArray;
use crate::geometry::storage::{RowSelection, row_selection_from_logical_rows};
use crate::py::arrow_c::column::OwnedArrowColumn;
use crate::py::feature_selection::select_feature_rows;
use crate::py::feature_values::snapshot_feature_rows;
use crate::py::row::{RowIndexOrSlice, parse_row_index_or_slice, slice_row_indices};

fn available<T>(value: Option<&T>) -> PyResult<&T> {
    value.ok_or_else(|| PyRuntimeError::new_err("feature property view was cleared"))
}

fn expose(value: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let py = value.py();
    if let Ok(dict) = value.cast_exact::<PyDict>() {
        return Ok(Py::new(py, FeaturePropertyMapping {
            values: Some(dict.clone().unbind()),
        })?
        .into_any());
    }
    if let Ok(tuple) = value.cast_exact::<PyTuple>() {
        let mut changed = false;
        let mut children = Vec::with_capacity(tuple.len());
        for child in tuple.iter() {
            let exposed = expose(&child)?;
            changed |= !exposed.bind(py).is(&child);
            children.push(exposed);
        }
        return if changed {
            Ok(PyTuple::new(py, children)?.into_any().unbind())
        } else {
            Ok(value.clone().unbind())
        };
    }
    Ok(value.clone().unbind())
}

fn thaw(value: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let py = value.py();
    if let Ok(view) = value.cast::<FeaturePropertyMapping>() {
        let data = available(view.borrow().values.as_ref())?.clone_ref(py);
        return thaw(data.bind(py).as_any());
    }
    if let Ok(mapping) = value.cast_exact::<PyDict>() {
        let output = mapping.copy()?;
        let mut nested = Vec::new();
        for (key, child) in output.iter() {
            if child.cast_exact::<PyDict>().is_ok() || child.cast_exact::<PyTuple>().is_ok() {
                nested.push((key.unbind(), child.unbind()));
            }
        }
        for (key, child) in nested {
            output.set_item(key, thaw(child.bind(py))?)?;
        }
        return Ok(output.into_any().unbind());
    }
    if let Ok(tuple) = value.cast_exact::<PyTuple>() {
        let children = tuple
            .iter()
            .map(|child| thaw(&child))
            .collect::<PyResult<Vec<_>>>()?;
        return Ok(PyList::new(py, children)?.into_any().unbind());
    }
    Ok(value.clone().unbind())
}

fn mapping_view(slf: &Bound<'_, FeaturePropertyMapping>, name: &str) -> PyResult<Py<PyAny>> {
    let py = slf.py();
    Ok(py
        .import("collections.abc")?
        .getattr(name)?
        .call1((slf,))?
        .unbind())
}

#[pyclass(
    name = "_FeaturePropertyMapping",
    module = "gometry._lib",
    immutable_type
)]
pub(crate) struct FeaturePropertyMapping {
    values: Option<Py<PyDict>>,
}

#[pymethods]
impl FeaturePropertyMapping {
    #[classattr]
    const __hash__: Option<Py<PyAny>> = None;

    fn __len__(&self, py: Python<'_>) -> PyResult<usize> {
        Ok(available(self.values.as_ref())?.bind(py).len())
    }

    fn __getitem__(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let value = available(self.values.as_ref())?
            .bind(py)
            .get_item(key)?
            .ok_or_else(|| PyKeyError::new_err(key.clone().unbind()))?;
        expose(&value)
    }

    fn __contains__(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<bool> {
        available(self.values.as_ref())?.bind(py).contains(key)
    }

    fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(available(self.values.as_ref())?
            .bind(py)
            .call_method0("__iter__")?
            .unbind())
    }

    #[pyo3(signature = (key, default = None))]
    fn get(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        default: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        available(self.values.as_ref())?
            .bind(py)
            .get_item(key)?
            .map_or_else(
                || Ok(default.map_or_else(|| py.None(), |value| value.clone().unbind())),
                |value| expose(&value),
            )
    }

    fn keys(slf: &Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        mapping_view(slf, "KeysView")
    }

    fn items(slf: &Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        mapping_view(slf, "ItemsView")
    }

    fn values(slf: &Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        mapping_view(slf, "ValuesView")
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(available(self.values.as_ref())?
            .bind(py)
            .repr()?
            .to_string_lossy()
            .into_owned())
    }

    fn __richcmp__(
        slf: &Bound<'_, Self>,
        other: &Bound<'_, PyAny>,
        op: CompareOp,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        if !matches!(op, CompareOp::Eq | CompareOp::Ne)
            || !other.is_instance(&py.import("collections.abc")?.getattr("Mapping")?)?
        {
            return Ok(py.NotImplemented());
        }
        let dict = py.import("builtins")?.getattr("dict")?;
        let left = dict.call1((slf,))?;
        let right = dict.call1((other,))?;
        Ok(left.rich_compare(right, op)?.unbind())
    }

    #[expect(
        clippy::same_name_method,
        reason = "PyO3 requires this name for cyclic-GC traversal"
    )]
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        visit.call(&self.values)
    }

    fn __clear__(slf: &Bound<'_, Self>) {
        let values = slf.borrow_mut().values.take();
        drop(values);
    }
}

#[pyclass(
    name = "_FeaturePropertyRows",
    module = "gometry._lib",
    sequence,
    immutable_type
)]
pub(crate) struct FeaturePropertyRows {
    data: Option<PropertyData>,
    selection: RowSelection,
    projection: Option<Py<PyDict>>,
}

type MixedSnapshot = (
    Option<Py<PyDict>>,
    Option<Py<PyTuple>>,
    Vec<Py<PyDict>>,
    Option<Py<PyTuple>>,
);

enum PropertyData {
    Rows(Py<PyTuple>),
    Tags {
        keys: Py<PyTuple>,
        values: Py<PyTuple>,
        offsets: Arc<[usize]>,
        pairs: Arc<[(usize, usize)]>,
        nulls: Option<Arc<[bool]>>,
    },
    Columns {
        columns: Py<PyDict>,
        len: usize,
    },
    Mixed {
        base: Py<FeaturePropertyRows>,
        additions: Py<PyTuple>,
        len: usize,
    },
    Concat {
        parts: Py<PyTuple>,
        ends: Arc<[usize]>,
    },
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "the checked physical row selects a part in the prefix-end inventory"
)]
fn concat_part<'py>(
    parts: &Bound<'py, PyTuple>,
    ends: &[usize],
    row: usize,
) -> PyResult<(PyRef<'py, FeaturePropertyRows>, usize)> {
    let index = ends.partition_point(|&end| end <= row);
    let start = index.checked_sub(1).map_or(0, |previous| ends[previous]);
    Ok((parts.get_item(index)?.extract()?, row - start))
}

fn column_value(column: &Bound<'_, PyAny>, physical: usize) -> PyResult<Py<PyAny>> {
    let value = column.get_item(physical)?;
    if column.cast_exact::<PyTuple>().is_ok() || column.cast_exact::<OwnedArrowColumn>().is_ok() {
        Ok(value.unbind())
    } else {
        Ok(column
            .py()
            .import("gometry._feature_values")?
            .getattr("column_value")?
            .call1((value,))?
            .unbind())
    }
}

impl FeaturePropertyRows {
    fn from_materialized_columns(
        py: Python<'_>,
        columns: &Bound<'_, PyDict>,
        len: usize,
    ) -> PyResult<Self> {
        let ndarray = py.import("numpy")?.getattr("ndarray")?;
        for (_, column) in columns.iter() {
            if column.is_instance(&ndarray)? {
                column.call_method1("setflags", (false,))?;
            }
        }
        Ok(Self::from_columns(columns, len))
    }

    fn select_admitted(&self, py: Python<'_>, selected: &RowSelection) -> PyResult<Self> {
        let selection = if selected.is_identity() {
            self.selection.clone()
        } else if self.selection.is_identity() {
            selected.clone()
        } else {
            let physical_len = self.physical_len(py)?;
            let len = selected
                .as_deref()
                .len(self.selection.as_deref().len(physical_len));
            row_selection_from_logical_rows(
                self.selection.as_deref(),
                physical_len,
                (0..len).map(|index| selected.as_deref().physical(index)),
            )
        };
        Ok(Self {
            data: Some(self.clone_data(py)?),
            selection,
            projection: self.projection.as_ref().map(|names| names.clone_ref(py)),
        })
    }

    fn physical_len(&self, py: Python<'_>) -> PyResult<usize> {
        Ok(match available(self.data.as_ref())? {
            PropertyData::Rows(rows) => rows.bind(py).len(),
            PropertyData::Tags { offsets, .. } => offsets.len().saturating_sub(1),
            PropertyData::Columns { len, .. } | PropertyData::Mixed { len, .. } => *len,
            PropertyData::Concat { ends, .. } => ends.last().copied().unwrap_or(0),
        })
    }

    fn clone_data(&self, py: Python<'_>) -> PyResult<PropertyData> {
        Ok(match available(self.data.as_ref())? {
            PropertyData::Rows(rows) => PropertyData::Rows(rows.clone_ref(py)),
            PropertyData::Tags {
                keys,
                values,
                offsets,
                pairs,
                nulls,
            } => PropertyData::Tags {
                keys: keys.clone_ref(py),
                values: values.clone_ref(py),
                offsets: Arc::clone(offsets),
                pairs: Arc::clone(pairs),
                nulls: nulls.clone(),
            },
            PropertyData::Columns { columns, len } => PropertyData::Columns {
                columns: columns.clone_ref(py),
                len: *len,
            },
            PropertyData::Mixed {
                base,
                additions,
                len,
            } => PropertyData::Mixed {
                base: base.clone_ref(py),
                additions: additions.clone_ref(py),
                len: *len,
            },
            PropertyData::Concat { parts, ends } => PropertyData::Concat {
                parts: parts.clone_ref(py),
                ends: Arc::clone(ends),
            },
        })
    }

    #[expect(
        clippy::arithmetic_side_effects,
        reason = "physical row indices are admitted against offsets containing one terminal entry"
    )]
    fn append_tags(
        &self,
        py: Python<'_>,
        index: usize,
        output: &Bound<'_, PyDict>,
        projection: Option<&Bound<'_, PyDict>>,
        excluded: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        let PropertyData::Tags {
            keys,
            values,
            offsets,
            pairs,
            ..
        } = available(self.data.as_ref())?
        else {
            unreachable!("feature tag data has tag storage");
        };
        let physical = self.selection.as_deref().physical(index);
        let start = *offsets.get(physical).expect("admitted feature row");
        let end = *offsets.get(physical + 1).expect("admitted feature row");
        for &(key, value) in pairs.get(start..end).expect("admitted feature tags") {
            let name = keys.bind(py).get_item(key)?;
            if let Some(selected) = projection
                && !selected.contains(&name)?
            {
                continue;
            }
            if let Some(seen) = excluded
                && seen.contains(&name)?
            {
                continue;
            }
            output.set_item(name, values.bind(py).get_item(value)?)?;
        }
        Ok(())
    }

    pub(crate) fn visit_values(
        &self,
        py: Python<'_>,
        index: usize,
        mut visit: impl FnMut(&Bound<'_, PyAny>, &Bound<'_, PyAny>) -> PyResult<()>,
    ) -> PyResult<()> {
        self.visit_values_filtered(py, index, None, None, &mut visit)
    }

    #[expect(
        clippy::arithmetic_side_effects,
        clippy::type_complexity,
        reason = "admitted row offsets include the terminal entry; the visitor borrows one Python key/value pair without retaining either"
    )]
    fn visit_values_filtered(
        &self,
        py: Python<'_>,
        index: usize,
        projection: Option<&Bound<'_, PyDict>>,
        excluded: Option<&Bound<'_, PyDict>>,
        visit: &mut dyn FnMut(&Bound<'_, PyAny>, &Bound<'_, PyAny>) -> PyResult<()>,
    ) -> PyResult<()> {
        let physical = self.selection.as_deref().physical(index);
        let own_projection = self.projection.as_ref().map(|names| names.bind(py));
        let mut emit = |name: &Bound<'_, PyAny>, value: &Bound<'_, PyAny>| -> PyResult<()> {
            if let Some(own_projection) = own_projection
                && !own_projection.contains(name)?
            {
                return Ok(());
            }
            if let Some(projection) = projection
                && !projection.contains(name)?
            {
                return Ok(());
            }
            if let Some(excluded) = excluded
                && excluded.contains(name)?
            {
                return Ok(());
            }
            visit(name, value)
        };
        match available(self.data.as_ref())? {
            PropertyData::Rows(rows) => {
                let row = rows.bind(py).get_item(physical)?;
                if !row.is_none() {
                    for (name, value) in row.cast_exact::<PyDict>()?.iter() {
                        emit(&name, &value)?;
                    }
                }
            },
            PropertyData::Columns { columns, .. } => {
                for (name, column) in columns.bind(py).iter() {
                    let value = column_value(&column, physical)?;
                    emit(&name, value.bind(py))?;
                }
            },
            PropertyData::Tags {
                keys,
                values,
                offsets,
                pairs,
                ..
            } => {
                let start = *offsets.get(physical).expect("admitted feature row");
                let end = *offsets.get(physical + 1).expect("admitted feature row");
                for &(key, value) in pairs.get(start..end).expect("admitted feature tags") {
                    emit(
                        &keys.bind(py).get_item(key)?,
                        &values.bind(py).get_item(value)?,
                    )?;
                }
            },
            PropertyData::Mixed {
                base, additions, ..
            } => {
                let shadowed = if let Some(excluded) = excluded {
                    excluded.copy()?
                } else {
                    PyDict::new(py)
                };
                for addition in additions.bind(py).iter() {
                    let addition = addition.extract::<PyRef<'_, Self>>()?;
                    let PropertyData::Columns { columns, .. } = available(addition.data.as_ref())?
                    else {
                        unreachable!("feature additions have column storage");
                    };
                    for name in columns.bind(py).keys().iter() {
                        shadowed.set_item(name, py.None())?;
                    }
                }
                base.bind(py).borrow().visit_values_filtered(
                    py,
                    physical,
                    projection,
                    Some(&shadowed),
                    &mut emit,
                )?;
                for addition in additions.bind(py).iter() {
                    addition
                        .extract::<PyRef<'_, Self>>()?
                        .visit_values_filtered(py, physical, projection, excluded, &mut emit)?;
                }
            },
            PropertyData::Concat { parts, ends } => {
                let (part, local) = concat_part(parts.bind(py), ends, physical)?;
                part.visit_values_filtered(py, local, None, None, &mut emit)?;
            },
        }
        Ok(())
    }

    fn append_columns(
        &self,
        py: Python<'_>,
        index: usize,
        output: &Bound<'_, PyDict>,
        projection: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        let PropertyData::Columns { columns, .. } = available(self.data.as_ref())? else {
            unreachable!("feature column groups have column storage");
        };
        let physical = self.selection.as_deref().physical(index);
        for (name, column) in columns.bind(py).iter() {
            if let Some(projection) = projection
                && !projection.contains(&name)?
            {
                continue;
            }
            output.set_item(name, column_value(&column, physical)?)?;
        }
        Ok(())
    }

    fn unshadowed_column_names<'py>(
        &self,
        py: Python<'py>,
        seen: &Bound<'py, PyDict>,
        projection: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let PropertyData::Columns { columns, .. } = available(self.data.as_ref())? else {
            unreachable!("feature column groups have column storage");
        };
        let mut names = Vec::new();
        for name in columns.bind(py).keys().iter() {
            if seen.contains(&name)? {
                continue;
            }
            if let Some(projection) = projection
                && !projection.contains(&name)?
            {
                continue;
            }
            seen.set_item(&name, py.None())?;
            names.push(name.unbind());
        }
        PyTuple::new(py, names)
    }

    fn row(&self, py: Python<'_>, index: usize) -> PyResult<Py<PyAny>> {
        let physical = self.selection.as_deref().physical(index);
        if self.is_null(py, index)? {
            return Ok(py.None());
        }
        if let PropertyData::Concat { parts, ends } = available(self.data.as_ref())? {
            let (part, local) = concat_part(parts.bind(py), ends, physical)?;
            let row = part.row(py, local)?;
            if row.bind(py).is_none() || self.projection.is_none() {
                return Ok(row);
            }
            let values = PyDict::new(py);
            for name in self
                .projection
                .as_ref()
                .expect("projection is present")
                .bind(py)
                .keys()
                .iter()
            {
                if row.bind(py).contains(&name)? {
                    values.set_item(&name, row.bind(py).get_item(&name)?)?;
                }
            }
            return expose(values.as_any());
        }
        if let PropertyData::Mixed {
            base, additions, ..
        } = available(self.data.as_ref())?
        {
            let projection = self.projection.as_ref().map(|names| names.bind(py));
            let base = base.bind(py).borrow();
            let values = PyDict::new(py);
            base.visit_values_filtered(py, physical, projection, None, &mut |name, value| {
                values.set_item(name, value)
            })?;
            for addition in additions.bind(py).iter() {
                addition
                    .extract::<PyRef<'_, Self>>()?
                    .append_columns(py, physical, &values, projection)?;
            }
            if let Some(projection) = projection {
                let filtered = PyDict::new(py);
                for name in projection.keys().iter() {
                    if let Some(value) = values.get_item(&name)? {
                        filtered.set_item(name, value)?;
                    }
                }
                return expose(filtered.as_any());
            }
            return expose(values.as_any());
        }
        if let Some(projection) = &self.projection {
            let values = PyDict::new(py);
            match available(self.data.as_ref())? {
                PropertyData::Rows(rows) => {
                    let row = rows.bind(py).get_item(physical)?;
                    if row.is_none() {
                        return Ok(py.None());
                    }
                    let row = row.cast_exact::<PyDict>()?;
                    for name in projection.bind(py).keys().iter() {
                        if let Some(value) = row.get_item(&name)? {
                            values.set_item(name, value)?;
                        }
                    }
                },
                PropertyData::Columns { columns, .. } => {
                    for name in projection.bind(py).keys().iter() {
                        let column = columns
                            .bind(py)
                            .get_item(&name)?
                            .ok_or_else(|| PyKeyError::new_err(name.clone().unbind()))?;
                        values.set_item(name, column_value(&column, physical)?)?;
                    }
                },
                PropertyData::Tags { .. } => {
                    self.append_tags(py, index, &values, Some(projection.bind(py)), None)?;
                },
                PropertyData::Mixed { .. } | PropertyData::Concat { .. } => unreachable!(),
            }
            return expose(values.as_any());
        }
        match available(self.data.as_ref())? {
            PropertyData::Rows(rows) => expose(&rows.bind(py).get_item(physical)?),
            PropertyData::Tags { .. } => {
                let values = PyDict::new(py);
                self.append_tags(py, index, &values, None, None)?;
                expose(values.as_any())
            },
            PropertyData::Columns { columns, .. } => {
                let values = PyDict::new(py);
                for (name, column) in columns.bind(py).iter() {
                    values.set_item(name, column_value(&column, physical)?)?;
                }
                expose(values.as_any())
            },
            PropertyData::Mixed { .. } | PropertyData::Concat { .. } => unreachable!(),
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "the selection maps checked rows into the admitted null mask"
    )]
    fn is_null(&self, py: Python<'_>, index: usize) -> PyResult<bool> {
        let physical = self.selection.as_deref().physical(index);
        match available(self.data.as_ref())? {
            PropertyData::Rows(rows) => Ok(rows.bind(py).get_item(physical)?.is_none()),
            PropertyData::Tags { nulls, .. } => {
                Ok(nulls.as_ref().is_some_and(|nulls| nulls[physical]))
            },
            PropertyData::Concat { parts, ends } => {
                let (part, local) = concat_part(parts.bind(py), ends, physical)?;
                part.is_null(py, local)
            },
            PropertyData::Columns { .. } | PropertyData::Mixed { .. } => Ok(false),
        }
    }

    #[expect(
        clippy::arithmetic_side_effects,
        reason = "physical row indices are admitted against offsets containing one terminal entry"
    )]
    fn cell(
        &self,
        py: Python<'_>,
        index: usize,
        name: &Bound<'_, PyString>,
    ) -> PyResult<Py<PyAny>> {
        let physical = self.selection.as_deref().physical(index);
        match available(self.data.as_ref())? {
            PropertyData::Rows(rows) => {
                let row = rows.bind(py).get_item(physical)?;
                if row.is_none() {
                    return Ok(py.None());
                }
                let mapping = row.cast_exact::<PyDict>()?;
                mapping
                    .get_item(name)?
                    .map_or_else(|| Ok(py.None()), |value| expose(&value))
            },
            PropertyData::Tags {
                keys,
                values,
                offsets,
                pairs,
                ..
            } => {
                let start = *offsets.get(physical).expect("admitted feature row");
                let end = *offsets.get(physical + 1).expect("admitted feature row");
                for &(key, value) in pairs.get(start..end).expect("admitted feature tags") {
                    if keys.bind(py).get_item(key)?.eq(name)? {
                        return expose(&values.bind(py).get_item(value)?);
                    }
                }
                Ok(py.None())
            },
            PropertyData::Columns { columns, .. } => {
                let column = columns
                    .bind(py)
                    .get_item(name)?
                    .ok_or_else(|| PyKeyError::new_err(name.clone().unbind()))?;
                expose(column_value(&column, physical)?.bind(py))
            },
            PropertyData::Mixed {
                base, additions, ..
            } => {
                for addition in additions.bind(py).iter().rev() {
                    let addition = addition.extract::<PyRef<'_, Self>>()?;
                    if addition.has_column(py, name)? {
                        return addition.cell(py, physical, name);
                    }
                }
                base.bind(py).borrow().cell(py, physical, name)
            },
            PropertyData::Concat { parts, ends } => {
                let (part, local) = concat_part(parts.bind(py), ends, physical)?;
                if part.has_column(py, name)? {
                    part.cell(py, local, name)
                } else {
                    Ok(py.None())
                }
            },
        }
    }

    #[expect(
        clippy::arithmetic_side_effects,
        reason = "physical row indices are admitted against offsets containing one terminal entry"
    )]
    fn has_column(&self, py: Python<'_>, name: &Bound<'_, PyString>) -> PyResult<bool> {
        if let Some(projection) = &self.projection {
            return projection.bind(py).contains(name);
        }
        match available(self.data.as_ref())? {
            PropertyData::Rows(rows) => {
                let rows = rows.bind(py);
                let len = self.selection.as_deref().len(rows.len());
                for index in 0..len {
                    let row = rows.get_item(self.selection.as_deref().physical(index))?;
                    if !row.is_none() && row.cast_exact::<PyDict>()?.contains(name)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            },
            PropertyData::Tags {
                keys,
                offsets,
                pairs,
                ..
            } => {
                let len = self
                    .selection
                    .as_deref()
                    .len(offsets.len().saturating_sub(1));
                for index in 0..len {
                    let physical = self.selection.as_deref().physical(index);
                    let start = *offsets.get(physical).expect("admitted feature row");
                    let end = *offsets.get(physical + 1).expect("admitted feature row");
                    for &(key, _) in pairs.get(start..end).expect("admitted feature tags") {
                        if keys.bind(py).get_item(key)?.eq(name)? {
                            return Ok(true);
                        }
                    }
                }
                Ok(false)
            },
            PropertyData::Columns { columns, .. } => columns.bind(py).contains(name),
            PropertyData::Mixed {
                base, additions, ..
            } => {
                for addition in additions.bind(py).iter() {
                    if addition
                        .extract::<PyRef<'_, Self>>()?
                        .has_column(py, name)?
                    {
                        return Ok(true);
                    }
                }
                let base = base.bind(py).borrow();
                if matches!(available(base.data.as_ref())?, PropertyData::Columns { .. }) {
                    return base.has_column(py, name);
                }
                let len = self.__len__(py)?;
                for index in 0..len {
                    let row = base.row(py, self.selection.as_deref().physical(index))?;
                    if !row.bind(py).is_none() && row.bind(py).contains(name)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            },
            PropertyData::Concat { .. } => {
                let mut found = false;
                for index in 0..self.__len__(py)? {
                    self.visit_values(py, index, |key, _| {
                        found |= key.eq(name)?;
                        Ok(())
                    })?;
                    if found {
                        return Ok(true);
                    }
                }
                Ok(false)
            },
        }
    }
}

#[pymethods]
impl FeaturePropertyRows {
    #[new]
    fn new(rows: &Bound<'_, PyTuple>) -> Self {
        Self {
            data: Some(PropertyData::Rows(rows.clone().unbind())),
            selection: RowSelection::Identity,
            projection: None,
        }
    }

    #[staticmethod]
    fn from_columns(columns: &Bound<'_, PyDict>, len: usize) -> Self {
        Self {
            data: Some(PropertyData::Columns {
                columns: columns.clone().unbind(),
                len,
            }),
            selection: RowSelection::Identity,
            projection: None,
        }
    }

    #[staticmethod]
    fn concatenate(py: Python<'_>, parts: Vec<Py<Self>>) -> PyResult<Self> {
        let mut ends = Vec::with_capacity(parts.len());
        let mut len = 0_usize;
        for part in &parts {
            len = len
                .checked_add(part.bind(py).borrow().__len__(py)?)
                .ok_or_else(|| {
                    pyo3::exceptions::PyOverflowError::new_err("feature count overflow")
                })?;
            ends.push(len);
        }
        Ok(Self {
            data: Some(PropertyData::Concat {
                parts: PyTuple::new(py, parts)?.unbind(),
                ends: ends.into(),
            }),
            selection: RowSelection::Identity,
            projection: None,
        })
    }

    #[pyo3(name = "_compact")]
    fn compact(&self, py: Python<'_>) -> PyResult<Self> {
        let len = self.__len__(py)?;
        let mut keys = Vec::new();
        let mut values = Vec::new();
        let mut pairs = Vec::new();
        let mut offsets = Vec::with_capacity(len.saturating_add(1));
        let mut nulls = Vec::with_capacity(len);
        offsets.push(0);
        for row in 0..len {
            nulls.push(self.is_null(py, row)?);
            self.visit_values(py, row, |name, value| {
                let index = keys.len();
                keys.push(name.clone().unbind());
                values.push(value.clone().unbind());
                pairs.push((index, index));
                Ok(())
            })?;
            offsets.push(pairs.len());
        }
        let nulls = nulls.iter().any(|&null| null).then(|| nulls.into());
        Ok(Self {
            data: Some(PropertyData::Tags {
                keys: PyTuple::new(py, keys)?.unbind(),
                values: PyTuple::new(py, values)?.unbind(),
                offsets: offsets.into(),
                pairs: pairs.into(),
                nulls,
            }),
            selection: RowSelection::Identity,
            projection: None,
        })
    }

    #[pyo3(name = "_materialize")]
    fn materialize(&self, py: Python<'_>) -> PyResult<Self> {
        let len = self.__len__(py)?;
        if let Some((columns, rows, groups, projection)) = self.mixed_snapshot(py, true)? {
            let mut result = if let Some(columns) = columns {
                Self::from_materialized_columns(py, columns.bind(py), len)?
            } else {
                let rows = rows.expect("mixed snapshots contain columns or rows");
                let rows = snapshot_feature_rows(py, rows.bind(py).as_any(), len)?;
                Self::new(rows.bind(py))
            };
            for columns in groups {
                let columns = Bound::new(
                    py,
                    Self::from_materialized_columns(py, columns.bind(py), len)?,
                )?;
                result = result.with_columns(py, &columns)?;
            }
            return if let Some(projection) = projection {
                result.project_admitted(py, projection.bind(py))
            } else {
                Ok(result)
            };
        }
        if let Some(columns) = self.column_snapshot(py, true)? {
            return Self::from_materialized_columns(py, columns.bind(py), len);
        }
        let mut result = self.compact(py)?;
        result.projection = self.projection.as_ref().map(|names| names.clone_ref(py));
        Ok(result)
    }

    fn __len__(&self, py: Python<'_>) -> PyResult<usize> {
        Ok(self.selection.as_deref().len(self.physical_len(py)?))
    }

    fn __getitem__(&self, py: Python<'_>, index: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let len = self.__len__(py)?;
        match parse_row_index_or_slice(index, len, "feature properties")? {
            RowIndexOrSlice::Index(index) => self.row(py, index),
            RowIndexOrSlice::Slice {
                start, step, count, ..
            } => {
                let values = slice_row_indices(start, step, count)
                    .map(|index| self.row(py, index))
                    .collect::<PyResult<Vec<_>>>()?;
                Ok(PyTuple::new(py, values)?.into_any().unbind())
            },
        }
    }

    #[pyo3(signature = (geometries, ids, id_column, index))]
    fn select(
        slf: &Bound<'_, Self>,
        geometries: &Bound<'_, PyGeometryArray>,
        ids: &Bound<'_, PyAny>,
        id_column: Option<&Bound<'_, PyString>>,
        index: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        select_feature_rows(slf.py(), geometries, slf.as_any(), ids, id_column, index)
    }

    fn column(
        slf: &Bound<'_, Self>,
        name: &Bound<'_, PyString>,
    ) -> PyResult<Py<FeaturePropertyColumn>> {
        let py = slf.py();
        if !slf.borrow().has_column(py, name)? {
            return Err(PyKeyError::new_err(name.clone().unbind()));
        }
        Py::new(py, FeaturePropertyColumn {
            rows: Some(slf.clone().unbind()),
            name: Some(name.clone().unbind()),
        })
    }

    #[pyo3(name = "_take")]
    fn take(&self, py: Python<'_>, indices: &Bound<'_, PyTuple>) -> PyResult<Self> {
        let physical_len = self.physical_len(py)?;
        let logical_len = self.selection.as_deref().len(physical_len);
        let selected = indices
            .iter()
            .map(|index| {
                let index = index.extract::<usize>()?;
                if index >= logical_len {
                    return Err(PyIndexError::new_err("feature property index out of range"));
                }
                Ok(index)
            })
            .collect::<PyResult<Vec<_>>>()?;
        let data = self.clone_data(py)?;
        Ok(Self {
            data: Some(data),
            selection: row_selection_from_logical_rows(
                self.selection.as_deref(),
                physical_len,
                selected,
            ),
            projection: self.projection.as_ref().map(|names| names.clone_ref(py)),
        })
    }

    fn project(&self, py: Python<'_>, names: &Bound<'_, PyTuple>) -> PyResult<Self> {
        let seen = PyDict::new(py);
        for name in names.iter() {
            let name = name.cast::<PyString>()?;
            if seen.contains(name)? {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "duplicate property column {:?}",
                    name.to_string_lossy()
                )));
            }
            seen.set_item(name, py.None())?;
            if !self.has_column(py, name)? {
                return Err(PyKeyError::new_err(name.clone().unbind()));
            }
        }
        self.project_admitted(py, names)
    }

    fn project_admitted(&self, py: Python<'_>, names: &Bound<'_, PyTuple>) -> PyResult<Self> {
        let projection = PyDict::new(py);
        for name in names.iter() {
            projection.set_item(name, py.None())?;
        }
        let data = match available(self.data.as_ref())? {
            PropertyData::Columns { columns, len } => {
                let selected = PyDict::new(py);
                for name in names.iter() {
                    let column = columns
                        .bind(py)
                        .get_item(&name)?
                        .ok_or_else(|| PyKeyError::new_err(name.clone().unbind()))?;
                    selected.set_item(name, column)?;
                }
                PropertyData::Columns {
                    columns: selected.unbind(),
                    len: *len,
                }
            },
            PropertyData::Rows(_)
            | PropertyData::Tags { .. }
            | PropertyData::Mixed { .. }
            | PropertyData::Concat { .. } => self.clone_data(py)?,
        };
        Ok(Self {
            data: Some(data),
            selection: self.selection.clone(),
            projection: Some(projection.unbind()),
        })
    }

    fn with_columns(&self, py: Python<'_>, additions: &Bound<'_, Self>) -> PyResult<Self> {
        let added = additions.borrow();
        let len = self.__len__(py)?;
        if added.__len__(py)? != len {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "column rows do not match feature rows",
            ));
        }
        let PropertyData::Columns { columns, .. } = available(added.data.as_ref())? else {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "expected column-backed properties",
            ));
        };
        if columns.bind(py).is_empty() {
            return Ok(Self {
                data: Some(self.clone_data(py)?),
                selection: self.selection.clone(),
                projection: self.projection.as_ref().map(|names| names.clone_ref(py)),
            });
        }
        let projection = if let Some(projection) = &self.projection {
            let names = projection.bind(py).copy()?;
            for name in columns.bind(py).keys().iter() {
                names.set_item(name, py.None())?;
            }
            Some(names.unbind())
        } else {
            None
        };
        let seen = PyDict::new(py);
        for name in columns.bind(py).keys().iter() {
            seen.set_item(name, py.None())?;
        }
        let retained = self.projection.as_ref().map(|names| names.bind(py));
        let (mut base, mut previous) = match available(self.data.as_ref())? {
            PropertyData::Mixed {
                base, additions, ..
            } => {
                let base = base
                    .bind(py)
                    .borrow()
                    .select_admitted(py, &self.selection)?;
                let mut previous = Vec::with_capacity(additions.bind(py).len().saturating_add(1));
                for part in additions.bind(py).iter().rev() {
                    let part = part.extract::<PyRef<'_, Self>>()?;
                    let names = part.unshadowed_column_names(py, &seen, retained)?;
                    if !names.is_empty() {
                        previous.push(Py::new(
                            py,
                            part.project_admitted(py, &names)?
                                .select_admitted(py, &self.selection)?,
                        )?);
                    }
                }
                previous.reverse();
                (base, previous)
            },
            PropertyData::Rows(_)
            | PropertyData::Tags { .. }
            | PropertyData::Columns { .. }
            | PropertyData::Concat { .. } => (
                Self {
                    data: Some(self.clone_data(py)?),
                    selection: self.selection.clone(),
                    projection: None,
                },
                Vec::new(),
            ),
        };
        if matches!(available(base.data.as_ref())?, PropertyData::Columns { .. }) {
            let names = base.unshadowed_column_names(py, &seen, retained)?;
            base = base.project_admitted(py, &names)?;
        }
        previous.push(additions.clone().unbind());
        Ok(Self {
            data: Some(PropertyData::Mixed {
                base: Py::new(py, base)?,
                additions: PyTuple::new(py, previous)?.unbind(),
                len,
            }),
            selection: RowSelection::Identity,
            projection,
        })
    }

    fn mutable_rows(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {
        let len = self.__len__(py)?;
        let values = (0..len)
            .map(|index| thaw(self.row(py, index)?.bind(py)))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(PyTuple::new(py, values)?.unbind())
    }

    #[pyo3(name = "_projection_snapshot")]
    fn projection_snapshot(&self, py: Python<'_>) -> PyResult<Option<Py<PyTuple>>> {
        self.projection
            .as_ref()
            .map(|names| PyTuple::new(py, names.bind(py).keys().iter()).map(Bound::unbind))
            .transpose()
    }

    #[pyo3(name = "_column_snapshot", signature = (*, detach = true))]
    fn column_snapshot(&self, py: Python<'_>, detach: bool) -> PyResult<Option<Py<PyDict>>> {
        let PropertyData::Columns { columns, len } = available(self.data.as_ref())? else {
            return Ok(None);
        };
        let selection = self.selection.as_deref();
        let logical_len = selection.len(*len);
        let output = PyDict::new(py);
        let mut arrow_indices: Option<Py<PyAny>> = None;
        let mut physical_rows: Option<Py<PyTuple>> = None;
        let names = self.projection.as_ref().unwrap_or(columns).bind(py).keys();
        for name in names.iter() {
            let name = &name;
            let column = columns
                .bind(py)
                .get_item(name)?
                .ok_or_else(|| PyKeyError::new_err(name.clone().unbind()))?;
            if !detach && selection.is_identity() {
                output.set_item(name, column)?;
                continue;
            }
            if let Ok(column) = column.cast_exact::<OwnedArrowColumn>() {
                let column = column.clone().unbind();
                let selection = &self.selection;
                let selected = py.detach(|| {
                    let selected = column.get().select(selection);
                    let selected = if detach {
                        selected.materialize()?
                    } else {
                        selected
                    };
                    Python::attach(|py| Py::new(py, selected))
                })?;
                output.set_item(name, selected)?;
                continue;
            }
            if let Ok(geometries) = column.cast_exact::<PyGeometryArray>() {
                let geometries = geometries.get();
                let selected = py
                    .detach(|| {
                        let selected = geometries.gather_logical_rows_iter(
                            (0..logical_len).map(|index| selection.physical(index)),
                        )?;
                        if detach {
                            selected.materialize_rows()
                        } else {
                            Ok(selected)
                        }
                    })
                    .map_err(PyErr::from)?;
                output.set_item(name, selected)?;
                continue;
            }
            if !detach && let Some(range) = selection.csr_rows(0..logical_len) {
                let slice = PySlice::new(
                    py,
                    isize::try_from(range.start)?,
                    isize::try_from(range.end)?,
                    1,
                );
                output.set_item(name, column.get_item(slice)?)?;
                continue;
            }
            if let Ok(values) = column.cast_exact::<PyTuple>() {
                let selected = (0..logical_len)
                    .map(|index| values.get_item(selection.physical(index)))
                    .collect::<PyResult<Vec<_>>>()?;
                output.set_item(name, PyTuple::new(py, selected)?)?;
            } else {
                let rows = if let Some(rows) = &physical_rows {
                    rows.clone_ref(py)
                } else {
                    let rows =
                        PyTuple::new(py, (0..logical_len).map(|index| selection.physical(index)))?
                            .unbind();
                    physical_rows = Some(rows.clone_ref(py));
                    rows
                };
                if column.is_instance(&py.import("numpy")?.getattr("ndarray")?)? {
                    output.set_item(name, column.call_method1("take", (rows,))?)?;
                    continue;
                }
                let indices = if let Some(indices) = &arrow_indices {
                    indices.clone_ref(py)
                } else {
                    let arrow = py.import("pyarrow")?;
                    let kwargs = PyDict::new(py);
                    kwargs.set_item("type", arrow.getattr("int64")?.call0()?)?;
                    let indices = arrow.getattr("array")?.call((rows,), Some(&kwargs))?;
                    let indices = indices.unbind();
                    arrow_indices = Some(indices.clone_ref(py));
                    indices
                };
                output.set_item(name, column.call_method1("take", (indices,))?)?;
            }
        }
        Ok(Some(output.unbind()))
    }

    #[pyo3(name = "_mixed_snapshot", signature = (*, detach = true))]
    fn mixed_snapshot(&self, py: Python<'_>, detach: bool) -> PyResult<Option<MixedSnapshot>> {
        let PropertyData::Mixed {
            base, additions, ..
        } = available(self.data.as_ref())?
        else {
            return Ok(None);
        };
        let len = self.__len__(py)?;
        let projection = self.projection.as_ref().map(|names| names.bind(py));
        let seen = PyDict::new(py);
        let mut groups = Vec::with_capacity(additions.bind(py).len());
        for addition in additions.bind(py).iter().rev() {
            let selected = addition
                .extract::<PyRef<'_, Self>>()?
                .select_admitted(py, &self.selection)?;
            let names = selected.unshadowed_column_names(py, &seen, projection)?;
            if !names.is_empty() {
                groups.push(
                    selected
                        .project_admitted(py, &names)?
                        .column_snapshot(py, detach)?
                        .expect("column additions retain their column storage"),
                );
            }
        }
        groups.reverse();
        let base = base
            .bind(py)
            .borrow()
            .select_admitted(py, &self.selection)?;
        let (columns, rows) = match available(base.data.as_ref())? {
            PropertyData::Columns { .. } => {
                let names = base.unshadowed_column_names(py, &seen, projection)?;
                (
                    base.project_admitted(py, &names)?
                        .column_snapshot(py, detach)?,
                    None,
                )
            },
            PropertyData::Rows(source) => {
                let source = source.bind(py);
                let mut rows = Vec::with_capacity(len);
                for index in 0..len {
                    let row = source.get_item(base.selection.as_deref().physical(index))?;
                    let result = PyDict::new(py);
                    if !row.is_none() {
                        for (name, value) in row.cast_exact::<PyDict>()?.iter() {
                            if seen.contains(&name)? {
                                continue;
                            }
                            if let Some(projection) = projection
                                && !projection.contains(&name)?
                            {
                                continue;
                            }
                            result.set_item(name, thaw(&value)?)?;
                        }
                    }
                    rows.push(result.unbind());
                }
                (None, Some(PyTuple::new(py, rows)?.unbind()))
            },
            PropertyData::Tags { .. } => {
                let mut rows = Vec::with_capacity(len);
                for index in 0..len {
                    let row = PyDict::new(py);
                    base.append_tags(py, index, &row, projection, Some(&seen))?;
                    rows.push(row.unbind());
                }
                (None, Some(PyTuple::new(py, rows)?.unbind()))
            },
            PropertyData::Concat { .. } => {
                let mut rows = Vec::with_capacity(len);
                for index in 0..len {
                    let row = PyDict::new(py);
                    base.visit_values_filtered(
                        py,
                        index,
                        projection,
                        Some(&seen),
                        &mut |name, value| row.set_item(name, thaw(value)?),
                    )?;
                    rows.push(row.unbind());
                }
                (None, Some(PyTuple::new(py, rows)?.unbind()))
            },
            PropertyData::Mixed { .. } => {
                unreachable!("feature column edits retain a flat base")
            },
        };
        Ok(Some((columns, rows, groups, self.projection_snapshot(py)?)))
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let len = self.__len__(py)?;
        let values = (0..len)
            .map(|index| self.row(py, index))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(PyTuple::new(py, values)?
            .repr()?
            .to_string_lossy()
            .into_owned())
    }

    fn __richcmp__(
        slf: &Bound<'_, Self>,
        other: &Bound<'_, PyAny>,
        op: CompareOp,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        if !matches!(op, CompareOp::Eq | CompareOp::Ne)
            || !other.is_instance(&py.import("collections.abc")?.getattr("Sequence")?)?
        {
            return Ok(py.NotImplemented());
        }
        let tuple = py.import("builtins")?.getattr("tuple")?;
        let left = tuple.call1((slf,))?;
        let right = tuple.call1((other,))?;
        Ok(left.rich_compare(right, op)?.unbind())
    }

    #[classattr]
    const __hash__: Option<Py<PyAny>> = None;

    #[expect(
        clippy::same_name_method,
        reason = "PyO3 requires this name for cyclic-GC traversal"
    )]
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        visit.call(&self.projection)?;
        match &self.data {
            Some(PropertyData::Rows(rows)) => visit.call(rows),
            Some(PropertyData::Tags { keys, values, .. }) => {
                visit.call(keys)?;
                visit.call(values)
            },
            Some(PropertyData::Columns { columns, .. }) => visit.call(columns),
            Some(PropertyData::Mixed {
                base, additions, ..
            }) => {
                visit.call(base)?;
                visit.call(additions)
            },
            Some(PropertyData::Concat { parts, .. }) => visit.call(parts),
            None => Ok(()),
        }
    }

    fn __clear__(slf: &Bound<'_, Self>) {
        let (data, projection) = {
            let mut rows = slf.borrow_mut();
            (rows.data.take(), rows.projection.take())
        };
        drop(data);
        drop(projection);
    }
}

#[pyclass(
    name = "_FeaturePropertyColumn",
    module = "gometry._lib",
    sequence,
    immutable_type
)]
pub(crate) struct FeaturePropertyColumn {
    rows: Option<Py<FeaturePropertyRows>>,
    name: Option<Py<PyString>>,
}

#[pymethods]
impl FeaturePropertyColumn {
    fn __len__(&self, py: Python<'_>) -> PyResult<usize> {
        available(self.rows.as_ref())?.bind(py).borrow().__len__(py)
    }

    fn __getitem__(&self, py: Python<'_>, index: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let rows = available(self.rows.as_ref())?.bind(py).borrow();
        let name = available(self.name.as_ref())?.bind(py);
        match parse_row_index_or_slice(index, rows.__len__(py)?, "feature column")? {
            RowIndexOrSlice::Index(index) => rows.cell(py, index, name),
            RowIndexOrSlice::Slice {
                start, step, count, ..
            } => {
                let values = slice_row_indices(start, step, count)
                    .map(|index| rows.cell(py, index, name))
                    .collect::<PyResult<Vec<_>>>()?;
                Ok(PyTuple::new(py, values)?.into_any().unbind())
            },
        }
    }

    #[pyo3(name = "_take")]
    fn take(&self, py: Python<'_>, indices: &Bound<'_, PyTuple>) -> PyResult<Self> {
        let selected = available(self.rows.as_ref())?
            .bind(py)
            .borrow()
            .take(py, indices)?;
        Ok(Self {
            rows: Some(Py::new(py, selected)?),
            name: Some(available(self.name.as_ref())?.clone_ref(py)),
        })
    }

    #[pyo3(name = "_snapshot", signature = (*, detach = true))]
    fn snapshot(&self, py: Python<'_>, detach: bool) -> PyResult<Py<PyAny>> {
        let name = available(self.name.as_ref())?.bind(py);
        let names = PyTuple::new(py, [name])?;
        let rows = available(self.rows.as_ref())?.bind(py).borrow();
        let columns = rows
            .project_admitted(py, &names)?
            .column_snapshot(py, detach)?
            .ok_or_else(|| {
                pyo3::exceptions::PyTypeError::new_err("expected a typed feature column")
            })?;
        columns
            .bind(py)
            .get_item(name)?
            .map(Bound::unbind)
            .ok_or_else(|| PyKeyError::new_err(name.clone().unbind()))
    }

    #[pyo3(name = "_materialize")]
    fn materialize(&self, py: Python<'_>) -> PyResult<Self> {
        let name = available(self.name.as_ref())?;
        let names = PyTuple::new(py, [name.bind(py)])?;
        let rows = available(self.rows.as_ref())?.bind(py).borrow();
        let rows = rows.project_admitted(py, &names)?.materialize(py)?;
        Ok(Self {
            rows: Some(Py::new(py, rows)?),
            name: Some(name.clone_ref(py)),
        })
    }

    fn __richcmp__(
        slf: &Bound<'_, Self>,
        other: &Bound<'_, PyAny>,
        op: CompareOp,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        if !matches!(op, CompareOp::Eq | CompareOp::Ne)
            || !other.is_instance(&py.import("collections.abc")?.getattr("Sequence")?)?
        {
            return Ok(py.NotImplemented());
        }
        let tuple = py.import("builtins")?.getattr("tuple")?;
        let left = tuple.call1((slf,))?;
        let right = tuple.call1((other,))?;
        Ok(left.rich_compare(right, op)?.unbind())
    }

    #[classattr]
    const __hash__: Option<Py<PyAny>> = None;

    #[expect(
        clippy::same_name_method,
        reason = "PyO3 requires this name for cyclic-GC traversal"
    )]
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        visit.call(&self.rows)?;
        visit.call(&self.name)
    }

    fn __clear__(slf: &Bound<'_, Self>) {
        let (rows, name) = {
            let mut column = slf.borrow_mut();
            (column.rows.take(), column.name.take())
        };
        drop(rows);
        drop(name);
    }
}

#[pyfunction(name = "_expose_feature_value")]
pub(crate) fn expose_feature_value(value: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    expose(value)
}

#[pyfunction(name = "_thaw_feature_value")]
pub(crate) fn thaw_feature_value(value: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    thaw(value)
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<FeaturePropertyMapping>()?;
    module.add_class::<FeaturePropertyRows>()?;
    module.add_class::<FeaturePropertyColumn>()?;
    module.add_function(wrap_pyfunction!(expose_feature_value, module)?)?;
    module.add_function(wrap_pyfunction!(thaw_feature_value, module)?)?;
    let abc = module.py().import("collections.abc")?;
    abc.getattr("Mapping")?
        .call_method1("register", (module.getattr("_FeaturePropertyMapping")?,))?;
    abc.getattr("Sequence")?
        .call_method1("register", (module.getattr("_FeaturePropertyRows")?,))?;
    abc.getattr("Sequence")?
        .call_method1("register", (module.getattr("_FeaturePropertyColumn")?,))?;
    Ok(())
}
