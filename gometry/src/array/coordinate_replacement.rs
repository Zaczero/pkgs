use pyo3::{PyResult, Python};

use crate::array::PyGeometryArray;
use crate::error::{BatchError, BatchResult, Result};
use crate::geometry::ShapeCountQuery;
use crate::geometry::storage::packed_columns::validate_replacement_ring;
use crate::geometry::storage::replacement::{
    CoordinateReplacement, replace_shape_coordinates, slice_replacement_for_shape,
};
use crate::geometry::storage::{GeometryArrayStorage, PackedStorage, ValidityMask};
#[cfg(test)]
use crate::test_support::boundary_probe;

impl PyGeometryArray {
    pub(crate) fn replace_coordinates_impl(
        &self,
        py: Python<'_>,
        replacement: CoordinateReplacement,
    ) -> PyResult<Self> {
        let shared_columns = !self.has_missing()
            && match self.storage() {
                GeometryArrayStorage::Packed(
                    PackedStorage::Points { row_map, .. } | PackedStorage::Lines { row_map, .. },
                ) => row_map.is_identity(),
                GeometryArrayStorage::Packed(PackedStorage::Polygons { .. })
                | GeometryArrayStorage::Mixed(_) => false,
            };
        if shared_columns {
            return Ok(self.replace_coordinates_native(&replacement)?);
        }
        Ok(py.detach(move || self.replace_coordinates_native(&replacement))?)
    }

    fn replace_coordinates_native(&self, replacement: &CoordinateReplacement) -> BatchResult<Self> {
        #[cfg(test)]
        boundary_probe::record("array replacement");
        let result = if let Some(dense) = self.replace_dense_packed_coordinates(replacement)? {
            dense
        } else if let Some(packed) = self.replace_packed_coords(replacement)? {
            packed
        } else {
            let mut present_cursor = 0_usize;
            let mut counts = ShapeCountQuery::default();
            let mut shapes = Vec::with_capacity(
                self.missing()
                    .map_or(self.len(), ValidityMask::present_count),
            );
            for row in self.row_operation().present_rows() {
                let logical = row.logical();
                let count = row.row().coordinates(&mut counts)?;
                let next_present_cursor = present_cursor.checked_add(count).ok_or_else(|| {
                    crate::error::Error::from(crate::error::ResourceError::SizeOverflow)
                })?;
                let sub = slice_replacement_for_shape(replacement, present_cursor, count);
                present_cursor = next_present_cursor;
                shapes.push(
                    row.row()
                        .try_with_shape(|shape| replace_shape_coordinates(shape, &sub))
                        .map_err(|error| BatchError::Row {
                            row: logical,
                            error,
                        })?,
                );
            }
            debug_assert_eq!(present_cursor, replacement.len);
            self.scatter_present_result(Self::from_shapes(shapes, self.frame.clone())?)?
        };
        #[cfg(test)]
        boundary_probe::record("array replacement finished");
        Ok(result)
    }

    fn replace_dense_packed_coordinates(
        &self,
        replacement: &CoordinateReplacement,
    ) -> Result<Option<Self>> {
        if self.has_missing() {
            return Ok(None);
        }
        let result = match self.storage() {
            GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map })
                if row_map.is_identity() =>
            {
                debug_assert_eq!(coords.len(), replacement.len);
                Self::packed_points(replacement.apply_to_seq(coords)?, self.frame.clone())?
            },
            GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords,
                offsets,
                row_map,
            }) if row_map.is_identity() => {
                debug_assert_eq!(coords.len(), replacement.len);
                Self::packed_lines(
                    replacement.apply_to_seq(coords)?,
                    offsets.clone(),
                    self.frame.clone(),
                )?
            },
            GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            }) if row_map.is_identity() => {
                debug_assert_eq!(coords.len(), replacement.len);
                let coords = replacement.apply_to_seq(coords)?;
                for [start, end] in ring_offsets.array_windows::<2>() {
                    validate_replacement_ring(&coords, *start as usize..*end as usize)?;
                }
                Self::packed_polygons(
                    coords,
                    ring_offsets.clone(),
                    polygon_offsets.clone(),
                    self.frame.clone(),
                )?
            },
            GeometryArrayStorage::Packed(
                PackedStorage::Points { .. }
                | PackedStorage::Lines { .. }
                | PackedStorage::Polygons { .. },
            )
            | GeometryArrayStorage::Mixed(_) => return Ok(None),
        };
        Ok(Some(result))
    }
}

#[cfg(test)]
mod tests {
    use pyo3::prelude::*;
    use pyo3::types::{PyCFunction, PyDict, PyTuple};

    use super::*;
    use crate::Frame;
    use crate::geometry::Shape;
    use crate::test_support::initialize_python;

    fn array(wkts: &[&str]) -> PyGeometryArray {
        PyGeometryArray::from_shapes(
            wkts.iter()
                .map(|wkt| crate::io::parse_wkt(wkt).unwrap())
                .collect(),
            Frame::None,
        )
        .unwrap()
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn application_boundaries_follow_storage_work_through_finalization() {
        initialize_python();
        Python::attach(|py| {
            let points = array(&["POINT ZM (1 2 3 4)", "POINT ZM (5 6 7 8)"]);
            let lines = array(&["LINESTRING (0 0, 1 1)", "LINESTRING (2 2, 3 3)"]);
            let polygons = array(&["POLYGON ((0 0, 1 0, 1 1, 0 0))"]);
            let mixed = array(&["POINT (1 2)", "LINESTRING (0 0, 1 1)"]);
            for (input, xs, expected, attached) in [
                (
                    points.clone(),
                    vec![10., 11.],
                    vec![Some("POINT ZM (10 2 3 4)"), Some("POINT ZM (11 6 7 8)")],
                    true,
                ),
                (
                    lines.clone(),
                    vec![10., 11., 12., 13.],
                    vec![
                        Some("LINESTRING (10 0, 11 1)"),
                        Some("LINESTRING (12 2, 13 3)"),
                    ],
                    true,
                ),
                (
                    polygons,
                    vec![10., 11., 11., 10.],
                    vec![Some("POLYGON ((10 0, 11 0, 11 1, 10 0))")],
                    false,
                ),
                (
                    lines.gather_logical_rows(&[1, 0, 1]).unwrap(),
                    vec![10., 11., 12., 13., 14., 15.],
                    vec![
                        Some("LINESTRING (10 2, 11 3)"),
                        Some("LINESTRING (12 0, 13 1)"),
                        Some("LINESTRING (14 2, 15 3)"),
                    ],
                    false,
                ),
                (
                    points
                        .with_missing_mask(ValidityMask::from_sparse(2, &[0]))
                        .unwrap(),
                    vec![10.],
                    vec![None, Some("POINT ZM (10 6 7 8)")],
                    false,
                ),
                (
                    mixed,
                    vec![10., 11., 12.],
                    vec![Some("POINT (10 2)"), Some("LINESTRING (11 0, 12 1)")],
                    false,
                ),
            ] {
                let kwargs = PyDict::new(py);
                kwargs.set_item("x", xs).unwrap();
                let (result, events) = boundary_probe::capture(|| {
                    input.with_coordinates(py, &PyTuple::empty(py), Some(&kwargs))
                });
                let result = result.unwrap();
                assert_eq!(events.first(), Some(&("array replacement", attached)));
                assert_eq!(
                    events.last(),
                    Some(&("array replacement finished", attached))
                );
                assert!(
                    events.iter().all(|(_, actual)| *actual == attached),
                    "{events:?}"
                );
                for (row, expected) in expected.into_iter().enumerate() {
                    assert_eq!(result.is_row_missing(row), expected.is_none());
                    if let Some(wkt) = expected {
                        assert_eq!(
                            result.geometry_at(row).unwrap().shape.shape(),
                            &crate::io::parse_wkt(wkt).unwrap()
                        );
                    }
                }
            }
        });
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn callbacks_stay_attached_and_run_once_before_native_application() {
        initialize_python();
        Python::attach(|py| {
            let input = Py::new(py, array(&["LINESTRING (0 0, 1 1)"]))
                .unwrap()
                .into_bound(py);
            let callback = PyCFunction::new_closure(py, None, None, |args, _kwargs| {
                boundary_probe::record("callback");
                args.get_item(0)?.call_method0("copy").map(Bound::unbind)
            })
            .unwrap();
            let (result, events) = boundary_probe::capture(|| {
                PyGeometryArray::map_coordinates(&input, py, callback.as_any())
            });
            assert_eq!(
                result.unwrap().geometry_at(0).unwrap().shape.shape(),
                &crate::io::parse_wkt("LINESTRING (0 0, 1 1)").unwrap()
            );
            assert_eq!(events, [
                ("callback", true),
                ("array replacement", true),
                ("array replacement finished", true),
            ]);

            let callback =
                PyCFunction::new_closure(py, None, None, |_args, _kwargs| -> PyResult<()> {
                    boundary_probe::record("callback");
                    Err(pyo3::exceptions::PyValueError::new_err("callback failed"))
                })
                .unwrap();
            let (result, events) = boundary_probe::capture(|| {
                PyGeometryArray::map_coordinates(&input, py, callback.as_any())
            });
            assert!(
                result
                    .unwrap_err()
                    .is_instance_of::<pyo3::exceptions::PyValueError>(py)
            );
            assert_eq!(events, [("callback", true)]);
        });
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn no_op_and_length_admission_precede_native_application() {
        initialize_python();
        Python::attach(|py| {
            let input = array(&["POLYGON ((0 0, 1 0, 1 1, 0 0))"]);
            let (result, events) =
                boundary_probe::capture(|| input.with_coordinates(py, &PyTuple::empty(py), None));
            assert!(events.is_empty());
            assert!(std::sync::Arc::ptr_eq(
                input.storage_arc(),
                result.unwrap().storage_arc()
            ));
            let kwargs = PyDict::new(py);
            kwargs.set_item("x", [10., 11., 12.]).unwrap();
            let (result, events) = boundary_probe::capture(|| {
                input.with_coordinates(py, &PyTuple::empty(py), Some(&kwargs))
            });
            let error = result.unwrap_err();
            assert!(error.to_string().contains("length"));
            assert!(!error.value(py).hasattr("__notes__").unwrap());
            assert!(events.is_empty());
            assert!(matches!(
                input.geometry_at(0).unwrap().shape.shape(),
                Shape::Polygon(_)
            ));
        });
    }
}
