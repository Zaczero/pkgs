use std::borrow::Cow;
use std::sync::Arc;

use crate::array::{ArrayRow, ComputedCaches, StorageLayout};
use crate::geometry::storage::GeometryArrayStorage;
use crate::geometry::{
    GeometryKind, Shape, TopologyDomain, admit_literal_geographic_shape,
    geographic_interchange_drawing, geographic_interchange_ready,
};
use crate::py::arrow_c::{NativeArrowExportError, NativeArrowExportResult};
use crate::{Frame, PyGeometryArray};

/// One publication feeds encoding selection, payload and GeoParquet metadata.
pub(crate) struct InterchangeArray<'a> {
    array: Cow<'a, PyGeometryArray>,
}

impl<'a> InterchangeArray<'a> {
    pub(crate) fn prepare(array: &'a PyGeometryArray) -> NativeArrowExportResult<Self> {
        let mut scratch = crate::dispatch::MetricScratch::default();
        let (domain, metric) = crate::dispatch::MetricResolver::GeographicCarry
            .resolve_topology_ctx(&array.frame, &mut scratch)?;
        let TopologyDomain::Geographic(frame) = domain else {
            return Ok(Self {
                array: Cow::Borrowed(array),
            });
        };
        let mut ready = true;
        for row in array.row_operation().rows_with_missing(|| ()) {
            if let ArrayRow::Present(row) = row {
                ready &= row
                    .row()
                    .try_with_shape(|shape| {
                        Ok::<_, crate::error::Error>(geographic_interchange_ready(shape, frame))
                    })
                    .map_err(|error| NativeArrowExportError::CoreRow {
                        row: row.logical(),
                        error,
                    })?;
            }
        }
        if ready {
            return Ok(Self {
                array: Cow::Borrowed(array),
            });
        }
        let metric_access = || -> crate::error::Result<&crate::crs::GeodesicMetric> {
            metric.geographic_chart_metric()?.ok_or_else(|| {
                crate::crs::CrsError::invalid("geographic interchange requires its source metric")
            })
        };
        let present_count = array
            .missing()
            .map_or(array.len(), |mask| mask.present_count());
        let mut shapes = Vec::with_capacity(present_count);
        for row in array.row_operation().rows_with_missing(|| ()) {
            if let ArrayRow::Present(row) = row {
                shapes.push(
                    row.row()
                        .try_with_shape(|shape| {
                            geographic_interchange_drawing(shape, frame, Some(&metric_access))
                        })
                        .map_err(|error| NativeArrowExportError::CoreRow {
                            row: row.logical(),
                            error,
                        })?,
                );
            }
        }
        promote_families(&mut shapes);
        let storage = PyGeometryArray::storage_from_shapes(shapes)?;
        let published = PyGeometryArray::from_result_storage(
            Arc::new(storage),
            array.frame.clone(),
            array.missing().cloned(),
            StorageLayout::PresentOnly,
            ComputedCaches::fresh(),
        )?;
        Ok(Self {
            array: Cow::Owned(published),
        })
    }

    pub(crate) fn array(&self) -> &PyGeometryArray {
        self.array.as_ref()
    }

    pub(crate) fn into_array(self) -> PyGeometryArray {
        self.array.into_owned()
    }
}

fn promote_families(shapes: &mut [Shape]) {
    let lines = shapes.iter().all(|shape| {
        matches!(
            shape,
            Shape::LineString(_)
                | Shape::MultiLineString(_)
                | Shape::Empty(GeometryKind::LineString | GeometryKind::MultiLineString, _)
        )
    });
    let multi_line = shapes.iter().any(|shape| {
        matches!(
            shape,
            Shape::MultiLineString(_) | Shape::Empty(GeometryKind::MultiLineString, _)
        )
    });
    if lines && multi_line {
        for shape in shapes {
            if let Shape::LineString(line) = shape {
                *shape = Shape::multi_line_string(line.as_coords().axes(), vec![line.clone()]);
            } else if let Shape::Empty(kind, _) = shape {
                *kind = GeometryKind::MultiLineString;
            }
        }
        return;
    }
    let polygons = shapes.iter().all(|shape| {
        matches!(
            shape,
            Shape::Polygon(_)
                | Shape::MultiPolygon(_)
                | Shape::Empty(GeometryKind::Polygon | GeometryKind::MultiPolygon, _)
        )
    });
    let multi_polygon = shapes.iter().any(|shape| {
        matches!(
            shape,
            Shape::MultiPolygon(_) | Shape::Empty(GeometryKind::MultiPolygon, _)
        )
    });
    if polygons && multi_polygon {
        for shape in shapes {
            if let Shape::Polygon(polygon) = shape {
                *shape = Shape::multi_polygon(polygon.axes(), vec![polygon.clone()]);
            } else if let Shape::Empty(kind, _) = shape {
                *kind = GeometryKind::MultiPolygon;
            }
        }
    }
}

pub(crate) fn admit_storage(
    storage: GeometryArrayStorage,
    frame: &Frame,
) -> crate::error::Result<GeometryArrayStorage> {
    let TopologyDomain::Geographic(geographic) = crate::geometry::topology_domain(frame)? else {
        return Ok(storage);
    };
    let mut ready = true;
    for shape in storage.iter_shapes() {
        ready &= geographic_interchange_ready(shape?.as_ref(), geographic);
    }
    if ready {
        return Ok(storage);
    }
    let mut shapes = Vec::with_capacity(storage.len());
    for shape in storage.iter_shapes() {
        shapes.push(admit_literal_geographic_shape(shape?.as_ref(), geographic)?);
    }
    PyGeometryArray::storage_from_shapes(shapes)
}
