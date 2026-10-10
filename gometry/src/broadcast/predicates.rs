//! Predicate broadcast dispatch: the scalar/array operand routing over the
//! unified [`Predicate`] table (`crate::predicates::engine`), plus the string/pattern
//! relate broadcasts and coordinate-input broadcasts. The generic value cores
//! live in the parent `broadcast`.

use pyo3::types::PyAny;

use crate::array::ArrayRow;
use crate::broadcast::{
    Bound, CollectRows as _, CoordinateInput, GeometryArrayStorage, GeometryErrorKind,
    GeometryInput, Point, PredicateSpec, Py, PyErr, PyGeometry, PyGeometryArray, PyResult, Python,
    Shape, ShapeData, broadcast_coordinate_input, classify_input, coordinate_input_with_expected,
    coordinate_sequence_len_hint, expected_geometry_or_array_for, mask_missing, py_bool,
    scalar_vs_shapes,
};
use crate::crs::frame::Frame;
use crate::error::Result;
use crate::geometry::source_support::PointMembership;
use crate::geometry::storage::{PackedStorage, ValidityMask, validity as missing};
use crate::geometry::{PointSet, PreparedPlan, TopologyDomain, storage};
use crate::predicates::engine;
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
    _frame: &Frame,
    domain: TopologyDomain,
) -> PyResult<Py<PyAny>> {
    let spec = *spec;
    if let Some(kind) = array.storage().uniform_kind() {
        let scalar_kind = crate::geometry::GeometryKind::of(scalar.shape.shape());
        let (left, right) = if scalar_is_left {
            (scalar_kind, kind)
        } else {
            (kind, scalar_kind)
        };
        if let Some(verdict) = spec.predicate.kind_gate(left, right) {
            return bool_array_mask_missing(py, vec![verdict; array.len()], array.missing());
        }
    }
    let missing = array.missing().cloned();
    let plan = scalar.prepared_plan();
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
        })
        .map_err(super::rows_err)?,
        missing.as_ref(),
    )
}

/// Equal-length arrays, evaluated pairwise through the shared matrix owner.
pub(crate) fn predicate_pairwise_arrays(
    py: Python<'_>,
    spec: &PredicateSpec,
    left: &PyGeometryArray,
    right: &PyGeometryArray,
    _frame: &Frame,
    domain: TopologyDomain,
    missing: Option<&ValidityMask>,
) -> PyResult<Py<PyAny>> {
    let spec = *spec;
    if let Some((left_kind, right_kind)) = left
        .storage()
        .uniform_kind()
        .zip(right.storage().uniform_kind())
        && let Some(verdict) = spec.predicate.kind_gate(left_kind, right_kind)
    {
        return bool_array_mask_missing(py, vec![verdict; left.len()], missing);
    }
    let left_bounds = left.existing_element_bounds();
    let right_bounds = right.existing_element_bounds();
    let left_array = left.clone();
    let right_array = right.clone();
    bool_array_mask_missing(
        py,
        py.detach(move || {
            let mut left_rows = left_array.row_operation();
            let mut right_rows = right_array.row_operation();
            macro_rules! pairs {
                ($index:ident, $left:ident, $right:ident, $evaluate:block) => {
                    left_rows
                        .rows_with_missing(|| ())
                        .zip(right_rows.rows_with_missing(|| ()))
                        .map(|(left, right)| {
                            let (ArrayRow::Present(left), ArrayRow::Present(right)) = (left, right)
                            else {
                                return Ok(false);
                            };
                            let $index = left.logical();
                            let $left = left.row();
                            let $right = right.row();
                            $evaluate
                        })
                        .collect_rows()
                };
            }
            match domain {
                TopologyDomain::Cartesian => {
                    pairs!(index, left, right, {
                        let left_gate = GateOperand::cartesian(
                            left,
                            left_bounds
                                .as_ref()
                                .and_then(|bounds| bounds.get(index).copied().flatten()),
                        );
                        let right_gate = GateOperand::cartesian(
                            right,
                            right_bounds
                                .as_ref()
                                .and_then(|bounds| bounds.get(index).copied().flatten()),
                        );
                        if let Some(verdict) = spec.pre_topology(&left_gate, &right_gate)? {
                            return Ok(verdict);
                        }
                        if matches!(spec.predicate, Predicate::Intersects | Predicate::Disjoint) {
                            if let Some(verdict) =
                                engine::row_point_intersection(&spec, left, right)?
                            {
                                return Ok(verdict);
                            }
                            let left = left_rows.prepared_at_with_bounds(
                                index,
                                left,
                                left_gate.bounds(),
                            )?;
                            let right = right_rows.prepared_at_with_bounds(
                                index,
                                right,
                                right_gate.bounds(),
                            )?;
                            engine::topology_pair_after_gates(
                                &spec,
                                &PreparedTopologyPair::cartesian(&left, &right),
                            )
                        } else {
                            crate::geometry::predicate_rows(left, right, spec.predicate)
                        }
                    })
                },
                TopologyDomain::Geographic(frame) => {
                    pairs!(index, left, right, {
                        if let Some(verdict) = spec.predicate.kind_gate(left.kind(), right.kind()) {
                            return Ok(verdict);
                        }
                        let left = left_rows.prepared_at(index, left)?;
                        let right = right_rows.prepared_at(index, right)?;
                        let left_cache = left_rows.frame_cache(index);
                        let right_cache = right_rows.frame_cache(index);
                        topology_scalar_pair(
                            &spec,
                            &PreparedTopologyPair::try_geographic(
                                &left,
                                &left_cache,
                                &right,
                                &right_cache,
                                frame,
                            )?,
                        )
                    })
                },
            }
        })
        .map_err(super::rows_err)?,
        missing,
    )
}

/// The split locations carried by a splitter array, in row order.
///
/// Packed point storage with a slot per logical row is already a coordinate
/// column, so its present rows are read straight out of it; compact nullable
/// and mixed storage visit rows instead.
pub(crate) fn splitter_points_from_array(splitters: &PyGeometryArray) -> Result<Vec<Point>> {
    let mut points = Vec::new();
    if let GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) =
        splitters.storage()
        // Compact nullable storage has no column slot for a missing row, so
        // its rows are reached through the row operation below, not by index.
        && !splitters.is_compact_nullable()
    {
        let map = row_map.as_deref();
        let logical_len = storage::point_logical_len(coords, map);
        points.reserve(
            splitters
                .missing()
                .map_or(logical_len, storage::ValidityMask::present_count),
        );
        points.extend(
            (0..logical_len)
                .filter(|&logical| !splitters.is_row_missing(logical))
                .map(|logical| coords.point_at(storage::physical_row(map, logical))),
        );
        return Ok(points);
    }
    for row in splitters.row_operation().present_rows() {
        // A compact nullable point column still yields point rows directly.
        if let storage::ShapeRow::Point(point) = row.row() {
            points.push(point);
            continue;
        }
        row.row().try_with_shape(|shape| {
            match shape
                .point_set()
                .ok_or(GeometryErrorKind::PointSplitterRequired)?
            {
                PointSet::Point(point) => points.push(point),
                PointSet::MultiPoint(items) => points.extend(items.iter()),
                // An empty point set contributes no split locations.
                PointSet::Empty => {},
            }
            Ok::<_, crate::error::Error>(())
        })?;
    }
    Ok(points)
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

fn relate_plan_pair(operands: &PreparedTopologyPair<'_>) -> crate::error::Result<String> {
    Ok(operands.relate()?.text())
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
    F: for<'a> Fn(PreparedTopologyPair<'a>) -> crate::error::Result<R> + Copy + Send + Sync,
{
    let output_missing = array.missing().cloned();
    let scalar_plan = scalar.prepared_plan();
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
                    let element_plan = rows.prepared_at(row, shape)?;
                    let element_cache = rows.frame_cache(row);
                    let operands = if scalar_is_left {
                        PreparedTopologyPair::try_for_domain(
                            &scalar_plan,
                            &scalar_cache,
                            &element_plan,
                            &element_cache,
                            domain,
                        )
                    } else {
                        PreparedTopologyPair::try_for_domain(
                            &element_plan,
                            &element_cache,
                            &scalar_plan,
                            &scalar_cache,
                            domain,
                        )
                    }?;
                    evaluate(operands)
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
    frame: &Frame,
    domain: TopologyDomain,
    evaluate: F,
) -> PyResult<Py<PyAny>>
where
    R: crate::broadcast::BulkElement,
    F: for<'a> Fn(PreparedTopologyPair<'a>) -> crate::error::Result<R> + Copy + Send + Sync,
{
    let missing = missing::union_many([left.missing(), right.missing()]);
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
                    let left_plan = left_rows.prepared_at(left_index, left_shape)?;
                    let right_plan = right_rows.prepared_at(right_index, right_shape)?;
                    let left_cache = left_rows.frame_cache(left_index);
                    let right_cache = right_rows.frame_cache(right_index);
                    evaluate(PreparedTopologyPair::try_for_domain(
                        &left_plan,
                        &left_cache,
                        &right_plan,
                        &right_cache,
                        domain,
                    )?)
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
    F: for<'a> Fn(PreparedTopologyPair<'a>) -> crate::error::Result<R> + Copy + Send + Sync,
{
    let left = crate::normalize_geometry_batch(py, left)?;
    let right = crate::normalize_geometry_batch(py, right)?;
    let admission = crate::dispatch::admit_binary(&left, &right, operation)?;
    let left = admission.left;
    let right = admission.right;
    let frame = admission.frame;
    let domain = admission.domain;
    match (left, right) {
        (GeometryInput::One(left), GeometryInput::One(right)) => {
            let left_plan = left.prepared_plan();
            let right_plan = right.prepared_plan();
            let left_cache = left.frame_cache();
            let right_cache = right.frame_cache();
            let value = py.detach(move || {
                PreparedTopologyPair::try_for_domain(
                    &left_plan,
                    &left_cache,
                    &right_plan,
                    &right_cache,
                    domain,
                )
                .and_then(evaluate)
            })?;
            R::into_py(value, py, &frame)
        },
        (GeometryInput::One(left), GeometryInput::Many(right)) => {
            topology_scalar_array(py, left, right, true, &frame, domain, evaluate)
        },
        (GeometryInput::Many(left), GeometryInput::One(right)) => {
            topology_scalar_array(py, right, left, false, &frame, domain, evaluate)
        },
        (GeometryInput::Many(left), GeometryInput::Many(right)) => {
            topology_array_pair(py, left, right, &frame, domain, evaluate)
        },
    }
}

/// `relate` matrix-string broadcast.
pub(crate) fn relate_string_broadcast(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    topology_broadcast(py, left, right, "relate", |pair| relate_plan_pair(&pair))
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
    let compiled = crate::geometry::CompiledPattern::new(pattern)
        .ok_or_else(|| invalid_de9im_pattern_err(pattern))?;
    topology_broadcast(py, left, right, "relate_pattern", move |operands| {
        engine::topology_scalar_pattern(compiled, &operands)
    })
}

/// Shared coordinate-query engine for the `contains` and `intersects`
/// keyword overloads. `inclusive` selects whether boundary contact matches.
pub(crate) fn xy_predicate(
    py: Python<'_>,
    geometry: &Bound<'_, PyAny>,
    x: &Bound<'_, PyAny>,
    y: &Bound<'_, PyAny>,
    inclusive: bool,
) -> PyResult<Py<PyAny>> {
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
                        let row = rows.prepared_at(logical, row)?;
                        if domain == TopologyDomain::Cartesian {
                            return cartesian_xy_matches(&row, point, inclusive);
                        }
                        let row_cache = rows.frame_cache(logical);
                        let point = PreparedPlan::new(ShapeData::from(Shape::Point(point)));
                        let point_cache = crate::geometry::FrameDependentCaches::default();
                        Ok(topology_scalar_pair(
                            &spec,
                            &PreparedTopologyPair::try_for_domain(
                                &row,
                                &row_cache,
                                &point,
                                &point_cache,
                                domain,
                            )?,
                        )?)
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
    if domain == TopologyDomain::Cartesian {
        let membership = if spec.predicate == Predicate::Intersects {
            PointMembership::BoundaryInclusive
        } else {
            PointMembership::Strict
        };
        let queries = original
            .prepare_point_queries(x.len().min(y.len()))
            .map_err(|error| (0, error))?;
        return x
            .into_iter()
            .zip(y)
            .map(|(x, y)| {
                let point = Point::new_raw_xy(x, y);
                queries.membership(point, membership)
            })
            .collect_rows();
    }
    x.into_iter()
        .zip(y)
        .map(|(x, y)| {
            let point = PreparedPlan::new(ShapeData::from(Shape::Point(Point::new_raw_xy(x, y))));
            let point_cache = crate::geometry::FrameDependentCaches::default();
            topology_scalar_pair(
                &spec,
                &PreparedTopologyPair::try_for_domain(
                    original,
                    original_cache,
                    &point,
                    &point_cache,
                    domain,
                )?,
            )
        })
        .collect_rows()
}

fn cartesian_xy_matches(prepared: &PreparedPlan, point: Point, inclusive: bool) -> Result<bool> {
    let membership = if inclusive {
        PointMembership::BoundaryInclusive
    } else {
        PointMembership::Strict
    };
    prepared.cartesian_point_membership(point, membership)
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
    let original = geometry.prepared_plan();
    let original_cache = geometry.frame_cache();
    if scalar {
        let (Some(x), Some(y)) = (x.into_iter().next(), y.into_iter().next()) else {
            return Err(InvalidGeometryError::new_err(
                "scalar x and y inputs must each contain a coordinate",
            ));
        };
        let point = Point::new(x, y)?;
        if domain == TopologyDomain::Cartesian {
            let matches = py.detach(move || cartesian_xy_matches(&original, point, inclusive))?;
            return Ok(py_bool(py, matches));
        }
        let point = PreparedPlan::new(ShapeData::from(Shape::Point(point)));
        let point_cache = crate::geometry::FrameDependentCaches::default();
        let value = py.detach(move || {
            PreparedTopologyPair::try_for_domain(
                &original,
                &original_cache,
                &point,
                &point_cache,
                domain,
            )
            .and_then(|operands| topology_scalar_pair(&spec, &operands))
        })?;
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
    use crate::crs::frame::crs_arc_static;
    use crate::geometry::{CoordSeq, Polygon, Ring};

    #[test]
    fn geographic_crossing_xy_agrees_with_scalar_oracle_at_seam_and_pole() {
        crate::test_support::initialize_python();
        let shape = Shape::Polygon(Polygon::new(
            Ring::from_closed(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(170.0, 80.0),
                    Point::new_raw_xy(-170.0, 80.0),
                    Point::new_raw_xy(-170.0, 90.0),
                    Point::new_raw_xy(170.0, 90.0),
                    Point::new_raw_xy(170.0, 80.0),
                ])
                .unwrap(),
            )
            .unwrap(),
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
        let source = geometry.prepared_plan();
        let domain = crate::geometry::topology_domain(&geometry.frame).unwrap();
        let expected: Vec<bool> = probes
            .iter()
            .map(|&(x, y)| {
                let point = PreparedPlan::new(Shape::Point(Point::new_raw_xy(x, y)));
                let point_cache = crate::geometry::FrameDependentCaches::default();
                PreparedTopologyPair::try_for_domain(
                    &source,
                    &geometry.frame_cache(),
                    &point,
                    &point_cache,
                    domain,
                )
                .expect("fixture admits")
                .relate()
                .unwrap()
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
