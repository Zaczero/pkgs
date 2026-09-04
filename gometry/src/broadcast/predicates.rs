#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Predicate broadcast dispatch: the scalar/array operand routing over the
//! unified [`Predicate`] table (`crate::predicates::engine`), plus the string/pattern
//! relate broadcasts and coordinate-input broadcasts. The generic value cores
//! live in the parent `broadcast`.

use pyo3::IntoPyObject as _;
use pyo3::types::PyAny;

use crate::array::{ArrayRow, ValidityMask};
use crate::boundary::metadata::Frame;
use crate::broadcast::{
    Bound, CollectRows as _, CoordSeq, CoordinateAxes, CoordinateInput, GeometryArrayStorage,
    GeometryErrorKind, GeometryInput, GeometryKind, Point, PredicateSpec, Py, PyErr, PyGeometry,
    PyGeometryArray, PyResult, Python, Shape, ShapeData, broadcast_coordinate_input,
    classify_input, classify_required, coordinate_input_with_expected,
    coordinate_sequence_len_hint, expected_geometry_or_array_for, mask_missing, paired_arrays,
    py_bool, scalar_vs_shapes,
};
use crate::error::Result;
use crate::geometry::{PreparedPlan, TopologyDomain};
use crate::predicates::PyPreparedGeometry;
use crate::predicates::engine::{
    GateOperand, Predicate, PreparedTopologyPair, topology_scalar_pair,
};
use crate::py::errors::{GeometryError, InvalidGeometryError};
use crate::py::numpy::bool_array;

pub(crate) fn bool_array_mask_missing(
    py: Python<'_>,
    mut values: Vec<bool>,
    missing: Option<&ValidityMask>,
) -> PyResult<Py<PyAny>> {
    mask_missing(&mut values, missing, false);
    bool_array(py, values)
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
        Some(prepared) => Some(prepared.get().as_geometry().into_pyobject(py)?),
        None => None,
    };
    let right_geometry = match right_prepared {
        Some(prepared) => Some(prepared.get().as_geometry().into_pyobject(py)?),
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
    let compatible = Frame::compatible_parts(
        scalar.crs_ref(),
        scalar.epoch(),
        array.crs_ref(),
        array.epoch(),
        spec.token,
    )?;
    let domain = crate::geometry::topology_domain(&compatible)?;
    let spec = *spec;
    let missing = array.missing().cloned();
    let plan = scalar.plan_or_transient();
    let scalar_cache = scalar.frame_cache();
    let array = array.clone();
    bool_array_mask_missing(
        py,
        py.detach(move || {
            scalar_vs_shapes(
                &spec,
                &plan,
                array
                    .row_operation()
                    .rows_with_missing(|| ())
                    .map(|row| match row {
                        ArrayRow::Missing { logical, .. } => (logical, None),
                        ArrayRow::Present(row) => (row.logical(), Some(row.row())),
                    }),
                scalar_is_left,
                Some(&array),
                domain,
                &scalar_cache,
            )
        }),
        missing.as_ref(),
    )
}

/// Equal-length arrays, evaluated pairwise through the shared matrix owner.
pub(crate) fn predicate_pairwise_arrays(
    py: Python<'_>,
    spec: &PredicateSpec,
    left: &PyGeometryArray,
    right: &PyGeometryArray,
) -> PyResult<Py<PyAny>> {
    let (..) = paired_arrays(left, right, spec.token)?;
    let domain = crate::geometry::topology_domain(&left.frame)?;
    let spec = *spec;
    let missing = crate::array::missing::union_pair(left.missing(), right.missing());
    let left_bounds = left.cached_element_bounds();
    let right_bounds = right.cached_element_bounds();
    let left_array = left.clone();
    let right_array = right.clone();
    bool_array_mask_missing(
        py,
        py.detach(move || {
            let mut left_rows = left_array.row_operation();
            let mut right_rows = right_array.row_operation();
            left_rows
                .rows_with_missing(|| ())
                .zip(right_rows.rows_with_missing(|| ()))
                .map(|(left, right)| {
                    // A missing row is a placeholder, not an empty geometry.
                    // It never reaches an exact kernel, and the mask replaces
                    // this value on the way out.
                    let (ArrayRow::Present(left), ArrayRow::Present(right)) = (left, right) else {
                        return false;
                    };
                    let index = left.logical();
                    let left = left.row();
                    let right = right.row();
                    if let Some(verdict) = spec.pre_topology(
                        GateOperand::new(
                            left,
                            left_bounds
                                .as_ref()
                                .and_then(|bounds| bounds.get(index).copied().flatten()),
                            domain,
                        ),
                        GateOperand::new(
                            right,
                            right_bounds
                                .as_ref()
                                .and_then(|bounds| bounds.get(index).copied().flatten()),
                            domain,
                        ),
                    ) {
                        return verdict;
                    }
                    let left = left_rows.prepared_at(index, left);
                    let right = right_rows.prepared_at(index, right);
                    let left_cache = left_rows.frame_cache(index);
                    let right_cache = right_rows.frame_cache(index);
                    topology_scalar_pair(
                        &spec,
                        PreparedTopologyPair::for_domain(
                            &left,
                            &left_cache,
                            &right,
                            &right_cache,
                            domain,
                        ),
                    )
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
        if splitters.is_compact_nullable() {
            let points = splitters
                .row_operation()
                .present_rows()
                .map(|row| match row.row() {
                    crate::array::ShapeRow::Point(point) => point.to_xy(),
                    _ => unreachable!("compact point storage yields point rows"),
                })
                .collect::<Vec<_>>();
            return Ok(Shape::multi_point(
                coords.axes(),
                CoordSeq::from_points(&points),
            ));
        }
        if row_map.is_identity() && !splitters.has_missing() {
            return Ok(Shape::multi_point(coords.axes(), coords.as_ref().clone()));
        }
        let map = row_map.as_deref();
        let points: Vec<_> = (0..crate::array::point_logical_len(coords, map))
            .filter(|&logical| !splitters.is_row_missing(logical))
            .map(|logical| coords.point_at(crate::array::physical_row(map, logical)))
            .collect();
        return Ok(Shape::multi_point(
            coords.axes(),
            CoordSeq::from_points(&points),
        ));
    }
    let mut points = Vec::new();
    for row in splitters.row_operation().present_rows() {
        row.row()
            .with_shape(|shape| -> Result<(), crate::error::Error> {
                match shape {
                    Shape::Point(point) => {
                        points.push(point.to_xy());
                        Ok(())
                    },
                    Shape::MultiPoint(items) => {
                        points.extend(items.iter().map(Point::to_xy));
                        Ok(())
                    },
                    // `POINT EMPTY` is point-like but contributes no split locations.
                    Shape::Empty(GeometryKind::Point, _) => Ok(()),
                    _ => Err(GeometryErrorKind::PointSplitterRequired.into()),
                }
            })?;
    }
    Ok(Shape::multi_point(
        CoordinateAxes::XY,
        CoordSeq::from_points(&points),
    ))
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

fn relate_plan_pair(operands: PreparedTopologyPair<'_>) -> String {
    operands.relate().text()
}

fn topology_scalar_array<R, F>(
    py: Python<'_>,
    scalar: &PyGeometry,
    array: &PyGeometryArray,
    scalar_is_left: bool,
    frame: &Frame,
    domain: TopologyDomain,
    evaluate: F,
) -> PyResult<Py<PyAny>>
where
    R: crate::broadcast::BulkElement,
    F: for<'a> Fn(PreparedTopologyPair<'a>) -> R + Copy + Send + Sync,
{
    let output_missing = array.missing().cloned();
    let scalar_plan = scalar.plan_or_transient();
    let scalar_cache = scalar.frame_cache();
    let array = array.clone();
    let values = py
        .detach(move || {
            let mut rows = array.row_operation();
            rows.rows_with_missing(R::missing_value)
                .map(|row| {
                    let ArrayRow::Present(present) = row else {
                        if let ArrayRow::Missing { value, .. } = row {
                            return Ok(value);
                        }
                        unreachable!();
                    };
                    let row = present.logical();
                    let shape = present.row();
                    let element_plan = rows.prepared_at(row, shape);
                    let element_cache = rows.frame_cache(row);
                    let operands = if scalar_is_left {
                        PreparedTopologyPair::for_domain(
                            &scalar_plan,
                            &scalar_cache,
                            &element_plan,
                            &element_cache,
                            domain,
                        )
                    } else {
                        PreparedTopologyPair::for_domain(
                            &element_plan,
                            &element_cache,
                            &scalar_plan,
                            &scalar_cache,
                            domain,
                        )
                    };
                    Ok(evaluate(operands))
                })
                .collect_rows()
        })
        .map_err(super::rows_err)?;
    R::bulk_into_py_masked(values, py, frame, output_missing.as_ref())
}

fn topology_array_pair<R, F>(
    py: Python<'_>,
    left: &PyGeometryArray,
    right: &PyGeometryArray,
    operation: &str,
    frame: &Frame,
    domain: TopologyDomain,
    evaluate: F,
) -> PyResult<Py<PyAny>>
where
    R: crate::broadcast::BulkElement,
    F: for<'a> Fn(PreparedTopologyPair<'a>) -> R + Copy + Send + Sync,
{
    paired_arrays(left, right, operation)?;
    let missing = crate::array::missing::union_pair(left.missing(), right.missing());
    let output_missing = missing;
    let left = left.clone();
    let right = right.clone();
    let values = py
        .detach(move || {
            let mut left_rows = left.row_operation();
            let mut right_rows = right.row_operation();
            left_rows
                .rows_with_missing(|| ())
                .zip(right_rows.rows_with_missing(|| ()))
                .map(|(left_row, right_row)| {
                    let (ArrayRow::Present(left), ArrayRow::Present(right)) = (left_row, right_row)
                    else {
                        return Ok(R::missing_value());
                    };
                    let left_index = left.logical();
                    let right_index = right.logical();
                    let left_shape = left.row();
                    let right_shape = right.row();
                    let left_plan = left_rows.prepared_at(left_index, left_shape);
                    let right_plan = right_rows.prepared_at(right_index, right_shape);
                    let left_cache = left_rows.frame_cache(left_index);
                    let right_cache = right_rows.frame_cache(right_index);
                    Ok(evaluate(PreparedTopologyPair::for_domain(
                        &left_plan,
                        &left_cache,
                        &right_plan,
                        &right_cache,
                        domain,
                    )))
                })
                .collect_rows()
        })
        .map_err(super::rows_err)?;
    R::bulk_into_py_masked(values, py, frame, output_missing.as_ref())
}

fn topology_broadcast<R, F>(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    operation: &str,
    evaluate: F,
) -> PyResult<Py<PyAny>>
where
    R: crate::broadcast::BulkElement,
    F: for<'a> Fn(PreparedTopologyPair<'a>) -> R + Copy + Send + Sync,
{
    let (left, right) = (classify_required(left)?, classify_required(right)?);
    let (left_crs, left_epoch) = match left {
        GeometryInput::One(geometry) => (geometry.crs_ref(), geometry.epoch()),
        GeometryInput::Many(array) => (array.crs_ref(), array.epoch()),
    };
    let (right_crs, right_epoch) = match right {
        GeometryInput::One(geometry) => (geometry.crs_ref(), geometry.epoch()),
        GeometryInput::Many(array) => (array.crs_ref(), array.epoch()),
    };
    let frame = Frame::compatible_parts(left_crs, left_epoch, right_crs, right_epoch, operation)?;
    let domain = crate::geometry::topology_domain(&frame)?;
    match (left, right) {
        (GeometryInput::One(left), GeometryInput::One(right)) => {
            let left_plan = left.plan_or_transient();
            let right_plan = right.plan_or_transient();
            let left_cache = left.frame_cache();
            let right_cache = right.frame_cache();
            let value = py.detach(move || {
                evaluate(PreparedTopologyPair::for_domain(
                    &left_plan,
                    &left_cache,
                    &right_plan,
                    &right_cache,
                    domain,
                ))
            });
            R::into_py(value, py, &frame)
        },
        (GeometryInput::One(left), GeometryInput::Many(right)) => {
            topology_scalar_array(py, left, right, true, &frame, domain, evaluate)
        },
        (GeometryInput::Many(left), GeometryInput::One(right)) => {
            topology_scalar_array(py, right, left, false, &frame, domain, evaluate)
        },
        (GeometryInput::Many(left), GeometryInput::Many(right)) => {
            topology_array_pair(py, left, right, operation, &frame, domain, evaluate)
        },
    }
}

/// `relate` matrix-string broadcast.
pub(crate) fn relate_string_broadcast(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    with_prepared_operands(py, left, right, |left, right| {
        topology_broadcast(py, left, right, "relate", relate_plan_pair)
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
        let compiled = crate::geometry::CompiledPattern::new(pattern)
            .ok_or_else(|| invalid_de9im_pattern_err(pattern))?;
        let bytes = compiled.bytes();
        topology_broadcast(py, left, right, "relate_pattern", move |operands| {
            crate::predicates::engine::topology_scalar_pattern(bytes, &operands)
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
        let geometry = prepared.get().as_geometry();
        return xy_predicate_geometry(py, &geometry, x, y, inclusive);
    }
    let input = classify_input(geometry).ok_or_else(|| expected_geometry_or_array_for(geometry))?;
    let receiver_len = match input {
        GeometryInput::One(_) => None,
        GeometryInput::Many(array) => Some(array.len()),
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
            let len = array.len();
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
    let domain = crate::geometry::topology_domain(&array.frame)?;
    let spec = if inclusive {
        Predicate::Intersects.spec()
    } else {
        Predicate::Contains.spec()
    };
    let values = py
        .detach(move || {
            let mut rows = array.row_operation();
            rows.rows_with_missing(|| ())
                .zip(x.into_iter().zip(y))
                .map(|(logical_row, (x, y))| match logical_row {
                    ArrayRow::Missing { .. } => Ok(false),
                    ArrayRow::Present(row) => {
                        let logical = row.logical();
                        let row = row.row();
                        let point = Point::new_raw_xy(x, y);
                        let row = rows.prepared_at(logical, row);
                        let row_cache = rows.frame_cache(logical);
                        let point = PreparedPlan::new(ShapeData::from(Shape::Point(point)));
                        let point_cache = crate::geometry::FrameDependentCaches::default();
                        Ok(topology_scalar_pair(
                            &spec,
                            PreparedTopologyPair::for_domain(
                                &row,
                                &row_cache,
                                &point,
                                &point_cache,
                                domain,
                            ),
                        ))
                    },
                })
                .collect_rows()
        })
        .map_err(super::rows_err)?;
    bool_array(py, values)
}

fn xy_array_values(
    x: Vec<f64>,
    y: Vec<f64>,
    original: &PreparedPlan,
    original_cache: &crate::geometry::FrameDependentCaches,
    spec: PredicateSpec,
    domain: TopologyDomain,
) -> Result<Vec<bool>, (usize, crate::error::Error)> {
    let points: Vec<Point> = x
        .into_iter()
        .zip(y)
        .map(|(x, y)| Point::new_raw_xy(x, y))
        .collect();
    points
        .into_iter()
        .map(|point| {
            let point = PreparedPlan::new(ShapeData::from(Shape::Point(point)));
            let point_cache = crate::geometry::FrameDependentCaches::default();
            Ok(topology_scalar_pair(
                &spec,
                PreparedTopologyPair::for_domain(
                    original,
                    original_cache,
                    &point,
                    &point_cache,
                    domain,
                ),
            ))
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
    let domain = crate::geometry::topology_domain(&geometry.frame)?;
    let original = geometry.plan_or_transient();
    let original_cache = geometry.frame_cache();
    if scalar {
        let point = Point::new(x[0], y[0])?;
        let point = PreparedPlan::new(ShapeData::from(Shape::Point(point)));
        let point_cache = crate::geometry::FrameDependentCaches::default();
        let value = py.detach(move || {
            topology_scalar_pair(
                &spec,
                PreparedTopologyPair::for_domain(
                    &original,
                    &original_cache,
                    &point,
                    &point_cache,
                    domain,
                ),
            )
        });
        return Ok(py_bool(py, value));
    }
    let result = py
        .detach(move || xy_array_values(x, y, &original, &original_cache, spec, domain))
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
            Ring::from_closed(CoordSeq::from(vec![
                Point::new_raw_xy(170.0, 80.0),
                Point::new_raw_xy(-170.0, 80.0),
                Point::new_raw_xy(-170.0, 90.0),
                Point::new_raw_xy(170.0, 90.0),
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
        let source = geometry.plan_or_transient();
        let domain = crate::geometry::topology_domain(&geometry.frame).unwrap();
        let expected: Vec<bool> = probes
            .iter()
            .map(|&(x, y)| {
                let point = PreparedPlan::new(Shape::Point(Point::new_raw_xy(x, y)));
                let point_cache = crate::geometry::FrameDependentCaches::default();
                PreparedTopologyPair::for_domain(
                    &source,
                    &geometry.frame_cache(),
                    &point,
                    &point_cache,
                    domain,
                )
                .relate()
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
