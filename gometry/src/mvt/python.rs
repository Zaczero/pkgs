//! Python boundary for local MVT layer admission and wire exchange.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use prost::Message as _;
use pyo3::IntoPyObjectExt as _;
use pyo3::exceptions::{PyKeyError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyString, PyTuple};

use super::{geometry, wire};
use crate::array::{ArrayRow, StreamingShapes};
use crate::error::ParseFormat;
use crate::py::errors::{GeometryError, parse_error};
use crate::py::feature_views::FeaturePropertyRows;
use crate::py::support::{OwnedBytes, exact_backed_bytes, features_type};
use crate::{Frame, PyGeometryArray, copy_one_byte_buffer};

#[pyclass(
    frozen,
    immutable_type,
    name = "_MVTSpatialIndex",
    module = "gometry._lib"
)]
struct MVTSpatialIndex {
    index: super::index::SourceIndex,
    periodic: bool,
}

#[pymethods]
impl MVTSpatialIndex {
    #[new]
    fn new(
        py: Python<'_>,
        geometries: &Bound<'_, PyGeometryArray>,
        periodic: bool,
    ) -> PyResult<Self> {
        let geometries = geometries.get();
        py.detach(|| {
            let mut bounds = Vec::new();
            for row in geometries.row_operation().rows_with_missing(|| ()) {
                let ArrayRow::Present(row) = row else {
                    continue;
                };
                let logical = row.logical();
                crate::check_detached_python_signals(logical)?;
                if let Some(bounds_value) = row
                    .row()
                    .try_with_shape(|shape| Ok::<_, crate::error::Error>(shape.bounds()))?
                {
                    bounds.push((logical, [
                        bounds_value.minx(),
                        bounds_value.miny(),
                        bounds_value.maxx(),
                        bounds_value.maxy(),
                    ]));
                }
            }
            Ok(Self {
                index: super::index::SourceIndex::new(bounds, periodic)?,
                periodic,
            })
        })
    }

    #[expect(
        clippy::needless_pass_by_value,
        reason = "PyO3 extracts the Python region sequence into an owned Vec"
    )]
    fn candidates(
        &self,
        py: Python<'_>,
        regions: Vec<[f64; 4]>,
        periodic: bool,
    ) -> PyResult<Vec<usize>> {
        if periodic && !self.periodic {
            return Err(PyValueError::new_err("source index has no periodic frame"));
        }
        if regions.iter().any(|[minx, miny, maxx, maxy]| {
            !minx.is_finite()
                || !miny.is_finite()
                || !maxx.is_finite()
                || !maxy.is_finite()
                || minx > maxx
                || miny > maxy
        }) {
            return Err(PyValueError::new_err("invalid source query bounds"));
        }
        Ok(py.detach(|| self.index.candidates(&regions, periodic)))
    }
}

#[derive(Hash, PartialEq, Eq)]
enum Scalar<'a> {
    String(Cow<'a, str>),
    Signed(i64),
    Unsigned(u64),
    Float(u64),
    Boolean(bool),
}

impl<'a> Scalar<'a> {
    fn from_python(
        value: &'a Bound<'_, PyAny>,
        path: impl Fn() -> String,
    ) -> PyResult<Option<Self>> {
        if value.is_none() {
            return Ok(None);
        }
        if value.is_instance_of::<PyBool>() {
            return Ok(Some(Self::Boolean(value.extract()?)));
        }
        if value.is_instance_of::<PyInt>() {
            if let Ok(value) = value.extract::<i64>() {
                return Ok(Some(Self::Signed(value)));
            }
            return value
                .extract::<u64>()
                .map(Self::Unsigned)
                .map(Some)
                .map_err(|_| {
                    PyTypeError::new_err(format!("{}: integer is outside MVT range", path()))
                });
        }
        if let Ok(value) = value.cast::<PyFloat>() {
            return Ok(Some(Self::Float(value.value().to_bits())));
        }
        if let Ok(value) = value.cast::<PyString>() {
            let value = value.to_str().map_err(|_| {
                PyTypeError::new_err(format!("{}: string must be valid UTF-8", path()))
            })?;
            return Ok(Some(Self::String(Cow::Borrowed(value))));
        }
        Err(PyTypeError::new_err(format!(
            "{}: unsupported MVT property value",
            path()
        )))
    }

    fn into_owned(self) -> Scalar<'static> {
        match self {
            Self::String(value) => Scalar::String(Cow::Owned(value.into_owned())),
            Self::Signed(value) => Scalar::Signed(value),
            Self::Unsigned(value) => Scalar::Unsigned(value),
            Self::Float(value) => Scalar::Float(value),
            Self::Boolean(value) => Scalar::Boolean(value),
        }
    }

    fn wire(&self) -> wire::Value {
        let mut value = wire::Value {
            string: None,
            float: None,
            double: None,
            int: None,
            uint: None,
            sint: None,
            boolean: None,
        };
        match self {
            Self::String(item) => value.string = Some(item.to_string()),
            Self::Signed(item) => value.int = Some(*item),
            Self::Unsigned(item) => value.uint = Some(*item),
            Self::Float(item) => value.double = Some(f64::from_bits(*item)),
            Self::Boolean(item) => value.boolean = Some(*item),
        }
        value
    }
}

#[expect(
    clippy::option_if_let_else,
    reason = "the ordered alternatives form one readable dispatch over the protobuf value fields"
)]
fn value_to_python(py: Python<'_>, value: &wire::Value) -> PyResult<Py<PyAny>> {
    if let Some(item) = &value.string {
        item.into_py_any(py)
    } else if let Some(item) = value.float {
        item.into_py_any(py)
    } else if let Some(item) = value.double {
        item.into_py_any(py)
    } else if let Some(item) = value.int {
        item.into_py_any(py)
    } else if let Some(item) = value.uint {
        item.into_py_any(py)
    } else if let Some(item) = value.sint {
        item.into_py_any(py)
    } else if let Some(item) = value.boolean {
        item.into_py_any(py)
    } else {
        Err(parse_error("invalid MVT value", ParseFormat::Mvt))
    }
}

fn feature_id(value: &Bound<'_, PyAny>, row: usize) -> PyResult<Option<u64>> {
    if value.is_none() {
        return Ok(None);
    }
    if value.is_instance_of::<PyBool>() || !value.is_instance_of::<PyInt>() {
        return Err(PyTypeError::new_err(format!(
            "feature {row} ID must be an unsigned integer"
        )));
    }
    value
        .extract::<u64>()
        .map(Some)
        .map_err(|_| PyTypeError::new_err(format!("feature {row} ID is outside the uint64 range")))
}

#[pyfunction(name = "_mvt_validate_inputs")]
fn validate_inputs(
    py: Python<'_>,
    geometries: &Bound<'_, PyGeometryArray>,
    properties: &Bound<'_, FeaturePropertyRows>,
    ids: &Bound<'_, PyAny>,
    rows: usize,
) -> PyResult<()> {
    let geometries = geometries.get();
    py.detach(|| {
        for row in geometries.row_operation().rows_with_missing(|| ()) {
            if let ArrayRow::Present(row) = row {
                let logical = row.logical();
                crate::check_detached_python_signals(logical)?;
                if row.row().try_with_shape(|shape| {
                    Ok::<_, crate::error::Error>(matches!(
                        shape,
                        crate::geometry::Shape::GeometryCollection(_)
                    ))
                })? {
                    return Err(GeometryError::new_err(format!(
                        "feature {logical} is a GeometryCollection"
                    )));
                }
            }
        }
        Ok::<_, PyErr>(())
    })?;
    let properties = properties.borrow();
    for row in 0..rows {
        crate::check_python_signals(py, row)?;
        feature_id(&ids.get_item(row)?, row)?;
        properties.visit_values(py, row, |key, value| {
            let key = key.cast::<PyString>()?.to_str().map_err(|_| {
                PyTypeError::new_err(format!("row {row}: property key must be valid UTF-8"))
            })?;
            Scalar::from_python(value, || format!("row {row}, field {key:?}"))?;
            Ok(())
        })?;
    }
    Ok(())
}

#[pyfunction(name = "_mvt_prepare_grid")]
#[expect(
    clippy::too_many_arguments,
    clippy::fn_params_excessive_bools,
    reason = "this private Python bridge transfers the preparation policy's independently typed fields"
)]
fn prepare_grid(
    py: Python<'_>,
    geometries: &Bound<'_, PyGeometryArray>,
    bounds: Option<(f64, f64, f64, f64)>,
    tile: Option<(u32, u32, u32)>,
    extent: u32,
    buffer: &Bound<'_, PyBytes>,
    clip: bool,
    raise_on_collapse: bool,
    tolerance: f64,
    douglas_peucker: bool,
    coverage: bool,
) -> PyResult<(Py<PyGeometryArray>, Vec<usize>)> {
    let grid = grid(bounds, tile, extent)?;
    let buffer = crate::geometry::buffer_from_bytes(buffer.as_bytes())?;
    let geometries = geometries.get();
    let (prepared, selected) = py.detach(|| {
        let mut output = StreamingShapes::with_capacity(0, geometries.len());
        let mut selected = Vec::new();
        let mut publish = |logical: usize, shape, collapsed| -> PyResult<()> {
            if collapsed && raise_on_collapse {
                return Err(GeometryError::new_err(format!(
                    "feature {logical} collapsed on the tile grid"
                )));
            }
            if let Some(shape) = shape {
                let shape = if tolerance == 0.0 || coverage {
                    shape
                } else {
                    grid.simplify_shape(
                        shape,
                        &buffer,
                        clip,
                        tolerance,
                        if douglas_peucker {
                            crate::geometry::SimplifyMethod::Dp
                        } else {
                            crate::geometry::SimplifyMethod::Vw
                        },
                    )?
                };
                let encoded = geometry::encode_shape(&shape).map_err(|error| {
                    GeometryError::new_err(format!("feature {logical}: {error}"))
                })?;
                if let Some(encoded) = encoded {
                    wire::validate_geometry(&encoded).map_err(|error| {
                        GeometryError::new_err(format!("feature {logical}: {error}"))
                    })?;
                    output.try_push(shape)?;
                    selected.push(logical);
                }
            }
            Ok(())
        };
        if coverage {
            let mut shapes = Vec::new();
            let mut logical_rows = Vec::new();
            for row in geometries.row_operation().rows_with_missing(|| ()) {
                if let ArrayRow::Present(row) = row {
                    crate::check_detached_python_signals(row.logical())?;
                    shapes.push(
                        row.row()
                            .try_with_shape(|shape| Ok::<_, crate::error::Error>(shape.clone()))?,
                    );
                    logical_rows.push(row.logical());
                }
            }
            let mut prepared = grid.prepare_coverage(&shapes, &buffer, clip, raise_on_collapse)?;
            if tolerance != 0.0 {
                let surviving: Vec<_> = prepared
                    .iter()
                    .filter_map(|(shape, _)| shape.clone())
                    .collect();
                if !surviving.is_empty() {
                    let simplified = grid.simplify_coverage(
                        &surviving,
                        &buffer,
                        clip,
                        tolerance,
                        if douglas_peucker {
                            crate::geometry::SimplifyMethod::Dp
                        } else {
                            crate::geometry::SimplifyMethod::Vw
                        },
                    )?;
                    let mut simplified = simplified.into_iter();
                    for (shape, _) in &mut prepared {
                        if shape.is_some() {
                            *shape = Some(
                                simplified
                                    .next()
                                    .expect("each surviving coverage row is simplified"),
                            );
                        }
                    }
                }
            }
            for (logical, (shape, collapsed)) in logical_rows.into_iter().zip(prepared) {
                publish(logical, shape, collapsed)?;
            }
        } else {
            for row in geometries.row_operation().rows_with_missing(|| ()) {
                let ArrayRow::Present(row) = row else {
                    continue;
                };
                let logical = row.logical();
                crate::check_detached_python_signals(logical)?;
                let (shape, collapsed) = row.row().try_with_shape(|shape| {
                    grid.prepare_shape(shape, &buffer, clip, raise_on_collapse)
                })?;
                publish(logical, shape, collapsed)?;
            }
        }
        Ok::<_, PyErr>((output.finish(Frame::default())?, selected))
    })?;
    Ok((Py::new(py, prepared)?, selected))
}

fn grid(
    bounds: Option<(f64, f64, f64, f64)>,
    tile: Option<(u32, u32, u32)>,
    extent: u32,
) -> PyResult<crate::geometry::TileGrid> {
    Ok(match (bounds, tile) {
        (Some(bounds), None) => {
            crate::geometry::TileGrid::custom(bounds.into(), i64::from(extent))?
        },
        (None, Some((zoom, column, row))) => {
            crate::geometry::TileGrid::xyz(zoom, column, row, i64::from(extent))?
        },
        _ => return Err(PyTypeError::new_err("choose exactly one tile or frame")),
    })
}

#[pyfunction(name = "_mvt_locate_grid")]
fn locate_grid(
    py: Python<'_>,
    geometries: &Bound<'_, PyGeometryArray>,
    bounds: Option<(f64, f64, f64, f64)>,
    tile: Option<(u32, u32, u32)>,
    extent: u32,
) -> PyResult<Py<PyGeometryArray>> {
    let grid = grid(bounds, tile, extent)?;
    let geometries = geometries.get();
    let output = py.detach(|| {
        let mut output = StreamingShapes::with_capacity(0, geometries.len());
        for row in geometries.row_operation().rows_with_missing(|| ()) {
            match row {
                ArrayRow::Missing { .. } => output.try_push_missing()?,
                ArrayRow::Present(row) => {
                    crate::check_detached_python_signals(row.logical())?;
                    output.try_push(row.row().try_with_shape(|shape| grid.locate_shape(shape))?)?;
                },
            }
        }
        output.finish(Frame::default()).map_err(PyErr::from)
    })?;
    Py::new(py, output)
}

fn encode_geometry_rows(
    geometries: &PyGeometryArray,
    emit: bool,
) -> PyResult<(Vec<wire::Feature>, Vec<usize>)> {
    let mut features = Vec::new();
    let mut logical_rows = Vec::new();
    for row in geometries.row_operation().rows_with_missing(|| ()) {
        let ArrayRow::Present(row) = row else {
            continue;
        };
        let logical = row.logical();
        crate::check_detached_python_signals(logical)?;
        let geometry = row
            .row()
            .try_with_shape(|shape| Ok::<_, crate::error::Error>(geometry::encode_shape(shape)))?
            .map_err(|error| GeometryError::new_err(format!("feature {logical}: {error}")))?;
        let Some(geometry) = geometry else {
            continue;
        };
        if emit {
            let (kind, geometry) = wire::encode_geometry(&geometry)
                .map_err(|error| GeometryError::new_err(format!("feature {logical}: {error}")))?;
            features.push(wire::Feature {
                id: None,
                tags: Vec::new(),
                kind: Some(kind),
                geometry,
            });
            logical_rows.push(logical);
        } else {
            wire::validate_geometry(&geometry)
                .map_err(|error| GeometryError::new_err(format!("feature {logical}: {error}")))?;
        }
    }
    Ok((features, logical_rows))
}

fn encode_layer(
    py: Python<'_>,
    name: &str,
    geometries: &PyGeometryArray,
    properties: &Bound<'_, FeaturePropertyRows>,
    ids: &Bound<'_, PyAny>,
    extent: u32,
    emit: bool,
) -> PyResult<wire::Layer> {
    if extent == 0 {
        return Err(PyTypeError::new_err("MVT extent must be positive"));
    }
    if geometries.crs_str().is_some() || geometries.epoch().is_some() {
        return Err(GeometryError::new_err(
            "local MVT geometry must not have a CRS or epoch",
        ));
    }
    let (features, logical_rows) = py.detach(|| encode_geometry_rows(geometries, emit))?;
    let mut layer = wire::Layer {
        name: Some(name.to_owned()),
        features,
        keys: Vec::new(),
        values: Vec::new(),
        extent: Some(extent),
        version: Some(2),
    };
    let mut keys = HashMap::<String, u32>::new();
    let mut values = HashMap::<Scalar<'static>, u32>::new();
    let properties = properties.borrow();
    let mut prepared = logical_rows
        .into_iter()
        .zip(layer.features.iter_mut())
        .peekable();
    for logical in 0..geometries.len() {
        crate::check_python_signals(py, logical)?;
        let id = feature_id(&ids.get_item(logical)?, logical)?;
        let mut feature = if prepared.peek().is_some_and(|(row, _)| *row == logical) {
            prepared.next().map(|(_, feature)| feature)
        } else {
            None
        };
        if let Some(feature) = &mut feature {
            feature.id = id;
        }
        properties.visit_values(py, logical, |key, value| {
            let field = key.cast::<PyString>()?.to_str().map_err(|_| {
                PyTypeError::new_err(format!(
                    "layer {name:?}, row {logical}: property key must be valid UTF-8"
                ))
            })?;
            let Some(value) = Scalar::from_python(value, || {
                format!("layer {name:?}, row {logical}, field {field:?}")
            })?
            else {
                return Ok(());
            };
            let Some(feature) = &mut feature else {
                return Ok(());
            };
            let key = if let Some(index) = keys.get(field) {
                *index
            } else {
                let index = u32::try_from(layer.keys.len())
                    .map_err(|_| GeometryError::new_err("MVT key dictionary is too large"))?;
                layer.keys.push(field.to_owned());
                keys.insert(field.to_owned(), index);
                index
            };
            let value = if let Some(index) = values.get(&value) {
                *index
            } else {
                let index = u32::try_from(layer.values.len())
                    .map_err(|_| GeometryError::new_err("MVT value dictionary is too large"))?;
                layer.values.push(value.wire());
                values.insert(value.into_owned(), index);
                index
            };
            feature.tags.extend([key, value]);
            Ok(())
        })?;
    }
    Ok(layer)
}

#[pyfunction(name = "_mvt_encode_layer")]
fn encode_layer_py(
    py: Python<'_>,
    name: &str,
    geometries: &Bound<'_, PyGeometryArray>,
    properties: &Bound<'_, FeaturePropertyRows>,
    ids: &Bound<'_, PyAny>,
    extent: u32,
) -> PyResult<Py<PyBytes>> {
    let layer = encode_layer(py, name, geometries.get(), properties, ids, extent, true)?;
    let tile = wire::Tile {
        layers: vec![layer],
    };
    PyBytes::new_with(py, tile.encoded_len(), |mut buffer| {
        py.detach(|| tile.encode_raw(&mut buffer));
        py.check_signals()
    })
    .map(Bound::unbind)
}

#[pyfunction(name = "_mvt_validate_layer")]
fn validate_layer_py(
    py: Python<'_>,
    geometries: &Bound<'_, PyGeometryArray>,
    properties: &Bound<'_, FeaturePropertyRows>,
    ids: &Bound<'_, PyAny>,
    extent: u32,
) -> PyResult<()> {
    encode_layer(py, "", geometries.get(), properties, ids, extent, false)?;
    Ok(())
}

#[pyfunction(name = "_mvt_decode_local")]
#[pyo3(signature = (data, layers = None, properties = None))]
#[expect(
    clippy::needless_pass_by_value,
    reason = "PyO3 extracts optional Python name sequences into owned vectors"
)]
fn decode_local(
    py: Python<'_>,
    data: &Bound<'_, PyAny>,
    layers: Option<Vec<String>>,
    properties: Option<Vec<String>>,
) -> PyResult<Py<PyDict>> {
    let bytes = if let Some(bytes) = exact_backed_bytes(data) {
        OwnedBytes::Backed(bytes)
    } else {
        OwnedBytes::Copied(copy_one_byte_buffer(data)?)
    };
    let bytes = bytes.as_slice();
    let requested = layers
        .as_ref()
        .map(|names| names.iter().cloned().collect::<HashSet<_>>());
    let selected = py
        .detach(|| wire::select_layers(bytes, requested.as_ref()))
        .map_err(|error| parse_error(error, ParseFormat::Mvt))?;
    if let Some(requested) = &layers {
        for name in requested {
            if !selected.iter().any(|(selected, _)| selected == name) {
                return Err(PyKeyError::new_err(name.clone()));
            }
        }
    }
    let properties = properties
        .as_ref()
        .map(|values| values.iter().map(String::as_str).collect::<Vec<_>>());
    let output = PyDict::new(py);
    for (name, body) in selected {
        let layer = py
            .detach(|| wire::decode_layer(body, properties.as_deref()))
            .map_err(|error| parse_error(format!("layer {name:?}: {error}"), ParseFormat::Mvt))?;
        let name = layer.name.clone();
        let geometries = py.detach(|| {
            let mut shapes = StreamingShapes::with_capacity(0, layer.features.len());
            for (row, feature) in layer.features.iter().enumerate() {
                crate::check_detached_python_signals(row)?;
                let shape = geometry::decode_shape(&feature.geometry).map_err(|error| {
                    parse_error(
                        format!("layer {name:?}, row {row}: {error}"),
                        ParseFormat::Mvt,
                    )
                })?;
                shapes.try_push(shape)?;
            }
            shapes.finish(Frame::default()).map_err(PyErr::from)
        })?;
        let mut ids = Vec::with_capacity(layer.features.len());
        let mut offsets = Vec::with_capacity(layer.features.len().saturating_add(1));
        let mut pairs = Vec::new();
        let mut key_indices = vec![None; layer.keys.len()];
        let mut value_indices = vec![None; layer.values.len()];
        let mut selected_keys = Vec::new();
        let mut selected_values = Vec::new();
        offsets.push(0);
        for (row, feature) in layer.features.into_iter().enumerate() {
            crate::check_python_signals(py, row)?;
            ids.push(feature.id);
            for (key, value) in feature.tags {
                let key_slot = key_indices.get_mut(key).expect("wire key index admitted");
                let key = if let Some(index) = *key_slot {
                    index
                } else {
                    let index = selected_keys.len();
                    selected_keys.push(
                        layer
                            .keys
                            .get(key)
                            .expect("wire key index admitted")
                            .into_py_any(py)?,
                    );
                    *key_slot = Some(index);
                    index
                };
                let value_slot = value_indices
                    .get_mut(value)
                    .expect("wire value index admitted");
                let value = if let Some(index) = *value_slot {
                    index
                } else {
                    let index = selected_values.len();
                    let value = layer
                        .values
                        .get(value)
                        .and_then(Option::as_ref)
                        .expect("selected wire value decoded");
                    selected_values.push(value_to_python(py, value)?);
                    *value_slot = Some(index);
                    index
                };
                pairs.push((key, value));
            }
            offsets.push(pairs.len());
        }
        let keys = PyTuple::new(py, selected_keys)?.unbind();
        let values = PyTuple::new(py, selected_values)?.unbind();
        let properties = Py::new(
            py,
            FeaturePropertyRows::from_tags(keys, values, offsets, pairs),
        )?;
        let role = ids.iter().any(Option::is_some).then_some("id");
        let ids = PyTuple::new(py, ids)?;
        let geometries = geometries.into_pyobject(py)?;
        let features = features_type(py)?
            .getattr("_from_property_rows")?
            .call1((geometries, properties, ids, role))?;
        output.set_item(name, (features, layer.extent))?;
    }
    Ok(output.unbind())
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<MVTSpatialIndex>()?;
    module.add_function(wrap_pyfunction!(encode_layer_py, module)?)?;
    module.add_function(wrap_pyfunction!(validate_layer_py, module)?)?;
    module.add_function(wrap_pyfunction!(decode_local, module)?)?;
    module.add_function(wrap_pyfunction!(validate_inputs, module)?)?;
    module.add_function(wrap_pyfunction!(prepare_grid, module)?)?;
    module.add_function(wrap_pyfunction!(locate_grid, module)?)?;
    Ok(())
}
