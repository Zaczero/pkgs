#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Predicate broadcast dispatch: the scalar/array operand routing over the
//! unified [`Predicate`] table (`crate::predicates::engine`), plus the string/pattern
//! relate broadcasts and coordinate-input broadcasts. The generic value cores
//! live in the parent `broadcast`.

use pyo3::types::PyAny;
use pyo3::{IntoPyObject as _, IntoPyObjectExt as _};

use crate::array::MissingMask;
use crate::boundary::metadata::Frame;
use crate::broadcast::{
    Arc, Bound, CollectRows as _, CoordSeq, CoordinateAxes, CoordinateInput, GeometryArrayStorage,
    GeometryErrorKind, GeometryInput, GeometryKind, Point, PredicateSpec, Py, PyErr, PyGeometry,
    PyGeometryArray, PyResult, Python, Shape, ShapeData, broadcast_coordinate_input, broadcast2,
    classify_input, coordinate_input_with_expected, coordinate_sequence_len_hint,
    expected_geometry_or_array_for, mask_missing, paired_arrays, py_bool, scalar_vs_shapes,
};
use crate::error::Result;
use crate::geometry::is_geographic_frame;
use crate::predicates::PyPreparedGeometry;
use crate::predicates::engine::{
    Predicate, skip_antimeridian_bounds_gate_row, topology_scalar_pair,
};
use crate::py::errors::{GeometryError, InvalidGeometryError};
use crate::py::numpy::bool_array;

pub(crate) fn bool_array_mask_missing(
    py: Python<'_>,
    mut values: Vec<bool>,
    missing: Option<&MissingMask>,
) -> PyResult<Py<PyAny>> {
    mask_missing(&mut values, missing, false);
    bool_array(py, values)
}

/// Whether a classified broadcast operand carries a geographic frame.
fn input_is_geographic(value: &Bound<'_, PyAny>) -> bool {
    match classify_input(value) {
        Some(GeometryInput::One(geometry)) => is_geographic_frame(&geometry.frame),
        Some(GeometryInput::Many(array)) => is_geographic_frame(&array.frame),
        None => false,
    }
}

/// Present prepared operands as their underlying geometries to a broadcast
/// implementation. The owned Python values keep the temporary unwrapped
/// operands alive for the duration of the callback.
fn with_prepared_operands(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    operation: impl for<'a> FnOnce(&Bound<'a, PyAny>, &Bound<'a, PyAny>) -> PyResult<Py<PyAny>>,
) -> PyResult<Py<PyAny>> {
    let left_prepared = left.cast::<PyPreparedGeometry>().ok();
    let right_prepared = right.cast::<PyPreparedGeometry>().ok();
    let left_geometry = match left_prepared {
        Some(prepared) => Some(prepared.get().geometry.clone().into_pyobject(py)?),
        None => None,
    };
    let right_geometry = match right_prepared {
        Some(prepared) => Some(prepared.get().geometry.clone().into_pyobject(py)?),
        None => None,
    };
    let left = left_geometry
        .as_ref()
        .map_or(left, |geometry| geometry.as_any());
    let right = right_geometry
        .as_ref()
        .map_or(right, |geometry| geometry.as_any());
    operation(left, right)
}

/// Broadcast one named predicate over `Geometry`/`GeometryArray` operands,
/// returning bool or ndarray.
///
/// All operand shapes route through the graph-matrix projection and shared
/// batch engine ([`scalar_vs_shapes`]).
pub(crate) fn predicate_broadcast(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    predicate: Predicate,
) -> PyResult<Py<PyAny>> {
    with_prepared_operands(py, left, right, |left, right| {
        crate::dispatch::dispatch_predicate(py, left, right, predicate)
    })
}

/// One fixed `scalar` operand against every element of `array`.
///
/// `scalar_is_left` names the operand order (`scalar OP element` vs
/// `element OP scalar`). Frame compatibility is checked once at the array
/// level; evaluation runs with the GIL released.
pub(crate) fn predicate_scalar_vs_array(
    py: Python<'_>,
    spec: &PredicateSpec,
    scalar: &PyGeometry,
    array: &PyGeometryArray,
    scalar_is_left: bool,
) -> PyResult<Py<PyAny>> {
    Frame::compatible_parts(
        scalar.crs_ref(),
        scalar.epoch(),
        array.crs_ref(),
        array.epoch(),
        spec.token,
    )?;
    let geographic = is_geographic_frame(&scalar.frame) || is_geographic_frame(&array.frame);
    let spec = *spec;
    let missing = array.missing().cloned();
    let shape = Arc::clone(&scalar.shape);
    let array = array.clone();
    bool_array_mask_missing(
        py,
        py.detach(move || {
            scalar_vs_shapes(
                &spec,
                &shape,
                array.storage().iter_rows().enumerate(),
                scalar_is_left,
                Some(&array),
                geographic,
            )
        }),
        missing.as_ref(),
    )
}

/// Equal-length arrays, evaluated pairwise through the shared matrix owner.
#[expect(
    clippy::too_many_lines,
    reason = "cohesive kernel; splitting obscures the algorithm"
)]
pub(crate) fn predicate_pairwise_arrays(
    py: Python<'_>,
    spec: &PredicateSpec,
    left: &PyGeometryArray,
    right: &PyGeometryArray,
) -> PyResult<Py<PyAny>> {
    let (left_shapes, right_shapes) = paired_arrays(left, right, spec.token)?;
    let geographic = is_geographic_frame(&left.frame) || is_geographic_frame(&right.frame);
    let spec = *spec;
    let missing = crate::array::missing::union_pair(left.missing(), right.missing());
    // Missing rows are placeholders, not empty geometries.  Decide this
    // before any packed fast path, bounds cache, or row materialization so a
    // null pair cannot enter an exact topology kernel.
    if missing.is_some() {
        let left_array = left.clone();
        let right_array = right.clone();
        return bool_array_mask_missing(
            py,
            py.detach(move || {
                left_shapes
                    .iter_rows()
                    .zip(right_shapes.iter_rows())
                    .enumerate()
                    .map(|(index, (left, right))| {
                        if left_array.is_row_missing(index) || right_array.is_row_missing(index) {
                            return false;
                        }
                        let left = left_array.prepared_row(index, left);
                        let right = right_array.prepared_row(index, right);
                        topology_scalar_pair(&spec, &left, &right, geographic)
                    })
                    .collect::<Vec<_>>()
            }),
            missing.as_ref(),
        );
    }
    // Bounds only provide refutations. Every unresolved pair uses the same
    // graph-matrix entry as scalar, prepared, and index evaluation.
    if let (Some(left_bounds), Some(right_bounds)) =
        (left.cached_element_bounds(), right.cached_element_bounds())
    {
        let left_array = left.clone();
        let right_array = right.clone();
        return bool_array_mask_missing(
            py,
            py.detach(move || {
                left_shapes
                    .iter_rows()
                    .zip(right_shapes.iter_rows())
                    .enumerate()
                    .map(|(index, (left, right))| {
                        // A mixed array can carry a point row without taking
                        // either packed-point fast lane.  Preserve the same
                        // fail-open pole rule there as well.
                        let pole_probe = geographic
                            && (left.with_shape(crate::geometry::shape_reaches_geographic_pole)
                                || right
                                    .with_shape(crate::geometry::shape_reaches_geographic_pole));
                        if !pole_probe
                            && !skip_antimeridian_bounds_gate_row(geographic, left)
                            && !skip_antimeridian_bounds_gate_row(geographic, right)
                            && let (Some(left_bounds), Some(right_bounds)) =
                                (left_bounds[index], right_bounds[index])
                            && let Some(verdict) = spec.bounds_gate(left_bounds, right_bounds)
                        {
                            return verdict;
                        }
                        let left = left_array.prepared_row(index, left);
                        let right = right_array.prepared_row(index, right);
                        topology_scalar_pair(&spec, &left, &right, geographic)
                    })
                    .collect::<Vec<_>>()
            }),
            missing.as_ref(),
        );
    }
    let left_array = left.clone();
    let right_array = right.clone();
    bool_array_mask_missing(
        py,
        py.detach(move || {
            left_shapes
                .iter_rows()
                .zip(right_shapes.iter_rows())
                .enumerate()
                .map(|(index, (left, right))| {
                    let left = left_array.prepared_row(index, left);
                    let right = right_array.prepared_row(index, right);
                    topology_scalar_pair(&spec, &left, &right, geographic)
                })
                .collect::<Vec<_>>()
        }),
        missing.as_ref(),
    )
}

pub(crate) fn multipoint_splitter_from_array(splitters: &PyGeometryArray) -> Result<Shape> {
    // Packed point arrays are already a uniform-axis column — clone it directly
    // rather than gathering every row and repacking through
    // `CoordSeq::from_points`.
    if let GeometryArrayStorage::Points { coords, row_map } = splitters.storage() {
        if row_map.is_identity() && !splitters.has_missing() {
            if coords.is_empty() {
                return Ok(Shape::typed_empty(GeometryKind::MultiPoint, coords.axes()));
            }
            return Ok(Shape::MultiPoint(coords.as_ref().clone().into()));
        }
        let map = row_map.as_deref();
        let points: Vec<_> = (0..crate::array::point_logical_len(coords, map))
            .filter(|&logical| !splitters.is_row_missing(logical))
            .map(|logical| coords.point_at(crate::array::physical_row(map, logical)))
            .collect();
        return Ok(if points.is_empty() {
            Shape::typed_empty(GeometryKind::MultiPoint, coords.axes())
        } else {
            Shape::MultiPoint(CoordSeq::from_points(&points).into())
        });
    }
    let mut points = Vec::new();
    for (missing, shape) in splitters.masked_shape_rows() {
        if missing {
            continue;
        }
        match &*shape {
            Shape::Point(point) => points.push(point.to_xy()),
            Shape::MultiPoint(items) => points.extend(items.iter().map(Point::to_xy)),
            // `POINT EMPTY` is point-like but contributes no split locations.
            Shape::Empty(GeometryKind::Point, _) => {},
            _ => return Err(GeometryErrorKind::PointSplitterRequired.into()),
        }
    }
    Ok(if points.is_empty() {
        Shape::typed_empty(GeometryKind::MultiPoint, CoordinateAxes::XY)
    } else {
        Shape::MultiPoint(points.into())
    })
}

pub(crate) fn ensure_same_len(left: usize, right: usize) -> PyResult<()> {
    if left == right {
        Ok(())
    } else {
        Err(GeometryError::new_err(format!(
            "GeometryArray operands must have the same length, got {left} and {right}"
        )))
    }
}

fn relate_shape_pair(geographic: bool, left: &Shape, right: &Shape) -> String {
    if geographic {
        crate::geometry::graph_relate_shapes_geographic(left, right).text()
    } else {
        crate::geometry::graph_relate_shapes(left, right).text()
    }
}

/// `relate` matrix-string broadcast.
pub(crate) fn relate_string_broadcast(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    with_prepared_operands(py, left, right, |left, right| {
        if let (Some(GeometryInput::One(left)), Some(GeometryInput::One(right))) =
            (classify_input(left), classify_input(right))
        {
            left.frame.compatible(&right.frame, "relate")?;
            return relate_shape_pair(
                is_geographic_frame(&left.frame),
                left.shape.shape(),
                right.shape.shape(),
            )
            .into_py_any(py);
        }
        let geographic = input_is_geographic(left) || input_is_geographic(right);
        broadcast2(py, left, right, "relate", move |left, right| {
            Ok(relate_shape_pair(geographic, left, right))
        })
    })
}

fn invalid_de9im_pattern_err(pattern: &str) -> PyErr {
    GeometryErrorKind::invalid_de9im_pattern(format!(
        "invalid DE-9IM pattern {pattern:?}; expected 9 characters of \
         'T', 'F', '0', '1', '2', or '*'"
    ))
    .into()
}

/// ``relate_pattern`` broadcast: compile the DE-9IM pattern once per call, not
/// once per array row.
pub(crate) fn relate_pattern_broadcast(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    pattern: &str,
) -> PyResult<Py<PyAny>> {
    with_prepared_operands(py, left, right, |left, right| {
        let pattern_owned = pattern.to_owned();
        let compiled = crate::geometry::CompiledPattern::new(&pattern_owned)
            .ok_or_else(|| invalid_de9im_pattern_err(pattern))?;
        if let (Some(GeometryInput::One(left)), Some(GeometryInput::One(right))) =
            (classify_input(left), classify_input(right))
        {
            left.frame.compatible(&right.frame, "relate_pattern")?;
            let matrix = if is_geographic_frame(&left.frame) {
                crate::geometry::graph_relate_shapes_geographic(
                    left.shape.shape(),
                    right.shape.shape(),
                )
            } else {
                crate::geometry::graph_relate_shapes(left.shape.shape(), right.shape.shape())
            };
            return Ok(py_bool(py, compiled.matches(matrix)));
        }
        let geographic = input_is_geographic(left) || input_is_geographic(right);
        broadcast2(py, left, right, "relate_pattern", move |left, right| {
            let matrix = if geographic {
                crate::geometry::graph_relate_shapes_geographic(left, right)
            } else {
                crate::geometry::graph_relate_shapes(left, right)
            };
            Ok(compiled.matches(matrix))
        })
    })
}

/// The shared `contains_xy`/`intersects_xy` engine. `inclusive` selects the
/// boundary-inclusive matrix projection (`intersects_xy`) over strict
/// containment (`contains_xy`).
pub(crate) fn xy_predicate(
    py: Python<'_>,
    geometry: &Bound<'_, PyAny>,
    x: &Bound<'_, PyAny>,
    y: &Bound<'_, PyAny>,
    inclusive: bool,
) -> PyResult<Py<PyAny>> {
    if let Ok(prepared) = geometry.cast::<PyPreparedGeometry>() {
        return xy_predicate_geometry(py, &prepared.get().geometry, x, y, inclusive);
    }
    let input = classify_input(geometry).ok_or_else(|| expected_geometry_or_array_for(geometry))?;
    let receiver_len = match input {
        GeometryInput::One(_) => None,
        GeometryInput::Many(array) => Some(array.storage().len()),
    };
    // Streaming owner: pin bare one-shot iterators with the sibling or receiver
    // length when known so each coordinate column is drained once (not twice via
    // independent `coordinate_input` + broadcast).
    let x_hint = coordinate_sequence_len_hint(x, "x")?;
    let y_hint = coordinate_sequence_len_hint(y, "y")?;
    let mut x = coordinate_input_with_expected(py, x, "x", y_hint.or(receiver_len), &|| {
        "x must be finite".into()
    })?;
    let mut y = coordinate_input_with_expected(py, y, "y", x_hint.or(receiver_len), &|| {
        "y must be finite".into()
    })?;
    if x.values.len() != y.values.len() && !x.scalar && !y.scalar {
        return Err(InvalidGeometryError::new_err(format!(
            "x and y must have the same length, got {} and {}",
            x.values.len(),
            y.values.len(),
        )));
    }
    match input {
        GeometryInput::One(geometry) => xy_predicate_geometry_inputs(py, geometry, x, y, inclusive),
        GeometryInput::Many(array) => {
            let len = array.storage().len();
            broadcast_coordinate_input(&mut x, len, "GeometryArray and x")?;
            broadcast_coordinate_input(&mut y, len, "GeometryArray and y")?;
            xy_predicate_array_values(py, array, x.values, y.values, inclusive)
        },
    }
}

/// Scalar-geometry entry used by both the public free functions and prepared
/// geometry methods. Keeping coordinate parsing/broadcasting here avoids
/// manufacturing a temporary Python geometry merely to re-enter dispatch.
pub(crate) fn xy_predicate_geometry(
    py: Python<'_>,
    geometry: &PyGeometry,
    x: &Bound<'_, PyAny>,
    y: &Bound<'_, PyAny>,
    inclusive: bool,
) -> PyResult<Py<PyAny>> {
    let x_hint = coordinate_sequence_len_hint(x, "x")?;
    let y_hint = coordinate_sequence_len_hint(y, "y")?;
    let x = coordinate_input_with_expected(py, x, "x", y_hint, &|| "x must be finite".into())?;
    let y = coordinate_input_with_expected(py, y, "y", x_hint, &|| "y must be finite".into())?;
    xy_predicate_geometry_inputs(py, geometry, x, y, inclusive)
}

fn xy_predicate_geometry_inputs(
    py: Python<'_>,
    geometry: &PyGeometry,
    mut x: CoordinateInput,
    mut y: CoordinateInput,
    inclusive: bool,
) -> PyResult<Py<PyAny>> {
    if x.values.len() != y.values.len() && !x.scalar && !y.scalar {
        return Err(InvalidGeometryError::new_err(format!(
            "x and y must have the same length, got {} and {}",
            x.values.len(),
            y.values.len(),
        )));
    }
    let scalar = x.scalar && y.scalar;
    let len = x.values.len().max(y.values.len());
    broadcast_coordinate_input(&mut x, len, "x and y")?;
    broadcast_coordinate_input(&mut y, len, "x and y")?;
    xy_predicate_values(py, geometry, x.values, y.values, scalar, inclusive)
}

fn xy_predicate_array_values(
    py: Python<'_>,
    array: &PyGeometryArray,
    x: Vec<f64>,
    y: Vec<f64>,
    inclusive: bool,
) -> PyResult<Py<PyAny>> {
    let array = array.clone();
    let geographic = is_geographic_frame(&array.frame);
    let spec = if inclusive {
        Predicate::Intersects.spec()
    } else {
        Predicate::Contains.spec()
    };
    let values = py
        .detach(move || {
            array
                .masked_storage_rows()
                .zip(x.into_iter().zip(y))
                .map(|((missing, row), (x, y))| {
                    if missing {
                        return Ok(false);
                    }
                    let point = Point::new_unchecked_xy(x, y);
                    let row = crate::array::PreparedRow::transient(row);
                    let point = ShapeData::from(Shape::Point(point));
                    Ok(topology_scalar_pair(&spec, &row, &point, geographic))
                })
                .collect_rows()
        })
        .map_err(super::rows_err)?;
    bool_array(py, values)
}

fn xy_array_values(
    x: Vec<f64>,
    y: Vec<f64>,
    original: &Arc<ShapeData>,
    spec: PredicateSpec,
    geographic: bool,
) -> Result<Vec<bool>, (usize, crate::error::Error)> {
    let points: Vec<Point> = x
        .into_iter()
        .zip(y)
        .map(|(x, y)| Point::new_unchecked_xy(x, y))
        .collect();
    points
        .into_iter()
        .map(|point| {
            let point = ShapeData::from(Shape::Point(point));
            Ok(topology_scalar_pair(&spec, original, &point, geographic))
        })
        .collect_rows()
}

pub(crate) fn xy_predicate_values(
    py: Python<'_>,
    geometry: &PyGeometry,
    x: Vec<f64>,
    y: Vec<f64>,
    scalar: bool,
    inclusive: bool,
) -> PyResult<Py<PyAny>> {
    let spec = if inclusive {
        Predicate::Intersects.spec()
    } else {
        Predicate::Contains.spec()
    };
    let geographic = is_geographic_frame(&geometry.frame);
    let original = Arc::clone(&geometry.shape);
    if scalar {
        let point = Point::new(x[0], y[0])?;
        let point = ShapeData::from(Shape::Point(point));
        return Ok(py_bool(
            py,
            topology_scalar_pair(&spec, &original, &point, geographic),
        ));
    }
    let result = py
        .detach(move || xy_array_values(x, y, &original, spec, geographic))
        .map_err(super::rows_err)?;
    bool_array(py, result)
}

#[cfg(test)]
mod geographic_tests {

    use pyo3::types::PyAnyMethods as _;

    use super::*;
    use crate::boundary::{Frame, crs_arc_static};
    use crate::geometry::{CoordSeq, Polygon, Ring};

    #[test]
    fn geographic_crossing_xy_agrees_with_scalar_oracle_at_seam_and_pole() {
        crate::test_support::initialize_python();
        let shape = Shape::Polygon(Polygon::new(
            Ring::from_trusted_closed(CoordSeq::from(vec![
                Point::new_unchecked_xy(170.0, 80.0),
                Point::new_unchecked_xy(-170.0, 80.0),
                Point::new_unchecked_xy(-170.0, 90.0),
                Point::new_unchecked_xy(170.0, 90.0),
            ])),
            Vec::new(),
        ));
        let frame = Frame::new(Some(crs_arc_static("EPSG:4326")), None)
            .expect("EPSG:4326 is a valid geographic frame");
        let geometry = PyGeometry::with_frame(shape, frame);

        // The first four points sit on or immediately beside both longitude
        // representations of the seam; the rest test the high-latitude cap.
        let probes = [
            (180.0, 85.0),
            (-180.0, 85.0),
            (179.999, 85.0),
            (-179.999, 85.0),
            (0.0, 90.0),
            (90.0, 90.0),
            (-90.0, 90.0),
        ];
        let x = probes.iter().map(|&(x, _)| x).collect();
        let y = probes.iter().map(|&(_, y)| y).collect();
        let expected: Vec<bool> = probes
            .iter()
            .map(|&(x, y)| {
                crate::geometry::graph_relate_shapes_geographic(
                    geometry.shape.shape(),
                    &Shape::Point(Point::new_unchecked_xy(x, y)),
                )
                .is_intersects()
            })
            .collect();

        let actual = Python::attach(|py| {
            let result = xy_predicate_values(py, &geometry, x, y, false, true)
                .expect("geographic XY predicate succeeds");
            result
                .bind(py)
                .call_method0("tolist")
                .expect("predicate returns an array with tolist")
                .extract::<Vec<bool>>()
                .expect("predicate array contains booleans")
        });

        assert_eq!(
            actual, expected,
            "geographic XY result differs from scalar oracle"
        );
    }
}
