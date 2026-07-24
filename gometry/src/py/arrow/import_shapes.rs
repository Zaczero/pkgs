#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::py::arrow::*;

fn append_arrow_coordseq_rows(
    py: Python<'_>,
    storage: &Bound<'_, PyAny>,
    crs: Option<&str>,
    geometries: &mut Vec<PyGeometry>,
    row: &mut usize,
    missing_rows: &mut Vec<usize>,
    build: impl Fn(CoordSeq) -> PyResult<Shape>,
) -> PyResult<()> {
    let len = storage.len()?;
    // Geometry-level nulls → missing rows (GeoArrow permits outer nulls only).
    let validity = arrow_validity(py, storage)?;
    let level = ArrowListLevel::read(py, storage)?;
    level.ensure(0, len)?;
    let values = storage.getattr("values")?;
    let (base, span) = coordinate_span(level.endpoint(0)?, level.endpoint(len)?)?;
    let coordinates = arrow_coordinate_values(py, &values, base, span)?;
    let crs = crs.map(crs_arc_str);
    for index in 0..len {
        if !validity.is_valid(index) {
            push_geometry_level_missing(geometries, missing_rows, *row, crs.clone());
            *row += 1;
            continue;
        }
        let range = level.range(index)?;
        let coords = coordinates.coordseq(range.start, range.end, *row)?;
        geometries.push(PyGeometry::from_shape_crs(build(coords)?, crs.clone()));
        *row += 1;
    }
    Ok(())
}

pub(crate) fn append_arrow_multipoints(
    py: Python<'_>,
    storage: &Bound<'_, PyAny>,
    crs: Option<&str>,
    geometries: &mut Vec<PyGeometry>,
    row: &mut usize,
    missing_rows: &mut Vec<usize>,
) -> PyResult<()> {
    append_arrow_coordseq_rows(py, storage, crs, geometries, row, missing_rows, |coords| {
        Ok(Shape::MultiPoint(coords))
    })
}

pub(crate) fn append_arrow_linestrings(
    py: Python<'_>,
    storage: &Bound<'_, PyAny>,
    crs: Option<&str>,
    geometries: &mut Vec<PyGeometry>,
    row: &mut usize,
    missing_rows: &mut Vec<usize>,
) -> PyResult<()> {
    append_arrow_coordseq_rows(py, storage, crs, geometries, row, missing_rows, |coords| {
        Ok(Shape::LineString(
            LineSeq::try_new(coords).map_err(PyErr::from)?,
        ))
    })
}
pub(crate) fn append_arrow_storage(
    py: Python<'_>,
    storage_array: &Bound<'_, PyAny>,
    storage: &ArrowStorage,
    geometries: &mut Vec<PyGeometry>,
    row: &mut usize,
    missing_rows: &mut Vec<usize>,
) -> PyResult<()> {
    match storage.encoding {
        GeometryEncoding::Point => append_arrow_points(
            py,
            storage_array,
            storage.crs.as_deref(),
            geometries,
            row,
            missing_rows,
        ),
        GeometryEncoding::MultiPoint => append_arrow_multipoints(
            py,
            storage_array,
            storage.crs.as_deref(),
            geometries,
            row,
            missing_rows,
        ),
        GeometryEncoding::LineString => append_arrow_linestrings(
            py,
            storage_array,
            storage.crs.as_deref(),
            geometries,
            row,
            missing_rows,
        ),
        GeometryEncoding::MultiLineString => append_arrow_multilinestrings(
            py,
            storage_array,
            storage.crs.as_deref(),
            geometries,
            row,
            missing_rows,
        ),
        GeometryEncoding::Polygon => append_arrow_polygons(
            py,
            storage_array,
            storage.crs.as_deref(),
            geometries,
            row,
            missing_rows,
        ),
        GeometryEncoding::MultiPolygon => append_arrow_multipolygons(
            py,
            storage_array,
            storage.crs.as_deref(),
            geometries,
            row,
            missing_rows,
        ),
        GeometryEncoding::Wkb => append_arrow_wkb(
            py,
            storage_array,
            storage.crs.as_deref(),
            storage.wkb_offset_width,
            geometries,
            row,
            missing_rows,
        ),
    }
}

pub(crate) struct ArrowGeometryInput {
    pub(crate) value: Py<PyAny>,
    pub(crate) field: Option<Py<PyAny>>,
}

pub(crate) fn arrow_geometry_input(
    pa: &Bound<'_, PyModule>,
    value: &Bound<'_, PyAny>,
) -> PyResult<ArrowGeometryInput> {
    if value.getattr("type").is_ok() || value.getattr("chunks").is_ok() {
        return Ok(ArrowGeometryInput {
            value: value.clone().unbind(),
            field: None,
        });
    }
    let Ok(names) = value.getattr("column_names") else {
        return Err(PyTypeError::new_err(
            "expected a GeoArrow-encoded Arrow array, ChunkedArray, Table, or RecordBatch",
        ));
    };
    let names = names.extract::<Vec<String>>()?;
    if names.is_empty() {
        return Err(PyTypeError::new_err(
            "Arrow table or record batch has no columns",
        ));
    }
    let schema = value.getattr("schema").ok();
    let geometry_name_indexes: Vec<usize> = names
        .iter()
        .enumerate()
        .filter_map(|(index, name)| (name == "geometry").then_some(index))
        .collect();
    if geometry_name_indexes.len() > 1 {
        return Err(PyTypeError::new_err(
            "Arrow table or record batch has multiple columns named 'geometry'; select one column explicitly",
        ));
    }
    if let Some(&index) = geometry_name_indexes.first() {
        return Ok(ArrowGeometryInput {
            value: value.call_method1("column", (index,))?.unbind(),
            field: schema
                .as_ref()
                .and_then(|schema| schema.call_method1("field", (index,)).ok())
                .map(Bound::unbind),
        });
    }
    let mut geometry_column = None;
    for index in 0..names.len() {
        let column = value.call_method1("column", (index,))?;
        let field = schema
            .as_ref()
            .and_then(|schema| schema.call_method1("field", (index,)).ok());
        if arrow_geometry_column_supported(pa, &column, field.as_ref()) {
            if geometry_column.is_some() {
                return Err(PyTypeError::new_err(
                    "Arrow table or record batch has multiple geometry-like columns; use table['geometry'] or select one column explicitly",
                ));
            }
            geometry_column = Some(ArrowGeometryInput {
                value: column.unbind(),
                field: field.map(Bound::unbind),
            });
        }
    }
    geometry_column.ok_or_else(|| {
        PyTypeError::new_err(
            "expected an Arrow geometry array, chunked array, or table/record batch with a 'geometry' column or exactly one geometry-like column",
        )
    })
}

pub(crate) fn arrow_geometry_column_supported(
    pa: &Bound<'_, PyModule>,
    value: &Bound<'_, PyAny>,
    field: Option<&Bound<'_, PyAny>>,
) -> bool {
    if let Ok(chunks) = value.getattr("chunks") {
        if let Ok(mut iter) = chunks.try_iter() {
            return iter.next().transpose().is_ok_and(|chunk| {
                chunk.map_or_else(
                    || arrow_value_frame(pa, value, field).is_ok(),
                    |chunk| arrow_storage_array(pa, &chunk, field).is_ok(),
                )
            });
        }
        return false;
    }
    arrow_storage_array(pa, value, field).is_ok()
}

pub(crate) fn append_arrow_points(
    py: Python<'_>,
    storage: &Bound<'_, PyAny>,
    crs: Option<&str>,
    geometries: &mut Vec<PyGeometry>,
    row: &mut usize,
    missing_rows: &mut Vec<usize>,
) -> PyResult<()> {
    let len = storage.len()?;
    // Geometry-level nulls (including ancestor-OR masks from parent structs)
    // become missing rows; vertex-level nulls still fail inside coordinate reads.
    let validity = arrow_validity(py, storage)?;
    // Visible coordinates are `[0, len)` of the (already array-offset-aware)
    // point struct; decode only that span.
    let coordinates = arrow_coordinate_values(py, storage, 0, len)?;
    let crs = crs.map(crs_arc_str);
    for index in 0..len {
        if !validity.is_valid(index) {
            push_geometry_level_missing(geometries, missing_rows, *row, crs.clone());
            *row += 1;
            continue;
        }
        geometries.push(PyGeometry::from_shape_crs(
            coordinates.point_shape(index, *row)?,
            crs.clone(),
        ));
        *row += 1;
    }
    Ok(())
}
