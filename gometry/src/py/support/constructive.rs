use std::sync::Arc;

use crate::geometry::Shape;
use crate::geometry::publication::PublicationRows;
use crate::geometry::storage::ValidityMask;
use crate::geometry::storage::builder::concat_packed_line_columns;
use crate::py::errors;
use crate::py::support::{
    Bound, Crs, Frame, FromPyObject, Polygon, PolygonizeFull, Py, PyAny, PyAnyMethods as _, PyErr,
    PyGeometryArray, PyResult, PyTypeError, Python, Result, VoronoiBoundary, exact_geometry,
    polygonize_result_type,
};
use crate::py::vectors::Groups;

/// Non-optional Voronoi `clip` input: Rust default is padded; explicit Python
/// ``None`` is rejected.
pub enum VoronoiClipInput {
    Default,
    Supplied(Py<PyAny>),
}

impl VoronoiClipInput {
    pub(crate) const DEFAULT: Self = Self::Default;
}

impl<'a, 'py> FromPyObject<'a, 'py> for VoronoiClipInput {
    type Error = PyErr;

    fn extract(value: pyo3::Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if value.is_none() {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "clip cannot be None",
            ));
        }
        Ok(Self::Supplied(value.as_any().clone().unbind()))
    }
}

/// Resolve a supplied Voronoi `clip` value: string modes `'padded'`/`'envelope'`,
/// or a `Polygon` to clip the diagram to.
pub(crate) fn voronoi_boundary_from_value<'a>(
    clip: &'a Bound<'_, PyAny>,
    subject_crs: Option<&Crs>,
    subject_epoch: Option<f64>,
    operation: &str,
) -> PyResult<VoronoiBoundary<'a>> {
    if let Some(geometry) = exact_geometry(clip) {
        let Shape::Polygon(polygon) = geometry.shape.shape() else {
            return Err(errors::geometry_type_err(
                "Voronoi clip geometry must be a Polygon",
            ));
        };
        // A `Polygon` clip is a second operand: it must share the subject's
        // CRS/epoch frame, like every other binary geometry op.
        Frame::compatible_parts(
            subject_crs,
            subject_epoch,
            geometry.crs_ref(),
            geometry.epoch(),
            operation,
        )?;
        return Ok(VoronoiBoundary::Polygon(polygon));
    }
    match clip.extract::<&str>() {
        Ok("padded") => Ok(VoronoiBoundary::Padded),
        Ok("envelope") => Ok(VoronoiBoundary::Envelope),
        Ok(value) => {
            let mut message = crate::tokens::unknown_token_message("Voronoi clip", value, &[
                "padded", "envelope",
            ]);
            message.push_str(", or a Polygon");
            Err(errors::parameter_error(message, "clip"))
        },
        Err(_) => Err(PyTypeError::new_err(
            "Voronoi clip must be 'padded', 'envelope', or a Polygon",
        )),
    }
}

/// Owned form used by detached scalar and array walks. Resolve the supplied
/// clip before detaching; arrays carry the same boundary through the existing
/// cumulative-budget helper across every row.
pub(crate) enum OwnedVoronoiBoundary {
    Padded,
    Envelope,
    Polygon(Polygon),
}

impl OwnedVoronoiBoundary {
    pub(crate) const fn as_borrowed(&self) -> VoronoiBoundary<'_> {
        match self {
            Self::Padded => VoronoiBoundary::Padded,
            Self::Envelope => VoronoiBoundary::Envelope,
            Self::Polygon(polygon) => VoronoiBoundary::Polygon(polygon),
        }
    }
}

pub(crate) fn owned_voronoi_boundary(
    py: Python<'_>,
    clip: &VoronoiClipInput,
    frame: &Frame,
    operation: &'static str,
) -> PyResult<OwnedVoronoiBoundary> {
    let supplied = match clip {
        VoronoiClipInput::Default => return Ok(OwnedVoronoiBoundary::Padded),
        VoronoiClipInput::Supplied(value) => value.bind(py),
    };
    Ok(
        match voronoi_boundary_from_value(supplied, frame.crs_ref(), frame.epoch(), operation)? {
            VoronoiBoundary::Padded => OwnedVoronoiBoundary::Padded,
            VoronoiBoundary::Envelope => OwnedVoronoiBoundary::Envelope,
            VoronoiBoundary::Polygon(polygon) => OwnedVoronoiBoundary::Polygon(polygon.clone()),
        },
    )
}

/// Shared engine for both Voronoi outputs across scalar and array inputs:
/// resolve `tolerance` and the clip boundary once against the subject frame,
/// run the per-shape diagram kernel, and publish results in that frame.
/// Run one Voronoi kernel over every input shape outside the interpreter
/// lock, one budget for the whole call. The boundary and shapes are fully
/// admitted Rust values by then; Python objects exist only after reattachment.
fn voronoi_outputs<T: Send, U: Send + pyo3::marker::Ungil>(
    py: Python<'_>,
    shapes: impl Iterator<Item = Arc<crate::geometry::ShapeData>>,
    frame: &Frame,
    topology_frame: Option<crate::geographic::GeographicFrame>,
    tolerance: f64,
    clip: &VoronoiClipInput,
    operation: &'static str,
    kernel: fn(
        &Shape,
        f64,
        VoronoiBoundary<'_>,
        Option<crate::geographic::GeographicFrame>,
        &mut crate::geometry::ExpansionBudget,
    ) -> Result<T>,
    mut publish: impl FnMut(
        T,
        &OwnedVoronoiBoundary,
        &mut crate::geometry::ExpansionBudget,
    ) -> Result<U>
    + pyo3::marker::Ungil,
) -> PyResult<Vec<U>> {
    let boundary = owned_voronoi_boundary(py, clip, frame, operation)?;
    let shapes = shapes.collect::<Vec<_>>();
    Ok(py.detach(move || {
        shapes
            .iter()
            .map(|shape| {
                let mut budget =
                    crate::geometry::ExpansionBudget::new(operation, "sites/clip topology");
                let output = kernel(
                    shape,
                    tolerance,
                    boundary.as_borrowed(),
                    topology_frame,
                    &mut budget,
                )?;
                publish(output, &boundary, &mut budget)
            })
            .collect::<Result<Vec<U>>>()
    })?)
}

pub(crate) fn voronoi_flatten(
    py: Python<'_>,
    shapes: impl Iterator<Item = Arc<crate::geometry::ShapeData>>,
    frame: Frame,
    tolerance: f64,
    clip: &VoronoiClipInput,
    operation: &'static str,
    kernel: fn(
        &Shape,
        f64,
        VoronoiBoundary<'_>,
        Option<crate::geographic::GeographicFrame>,
        &mut crate::geometry::ExpansionBudget,
    ) -> Result<crate::geometry::VoronoiPolygonOutputs>,
) -> PyResult<PyGeometryArray> {
    let geographic_frame = crate::geometry::topology_domain(&frame)
        .ok()
        .and_then(|domain| match domain {
            crate::geometry::TopologyDomain::Geographic(frame) => Some(frame),
            crate::geometry::TopologyDomain::Cartesian => None,
        });
    if let Some(geographic_frame) = geographic_frame {
        let output_frame = frame.clone();
        let mut finished = voronoi_outputs(
            py,
            shapes,
            &frame,
            Some(geographic_frame),
            tolerance,
            clip,
            operation,
            kernel,
            move |outputs, _boundary, budget| {
                let mut writer = PublicationRows::with_producer_drawing_capacity(0);
                writer.push_voronoi_polygon_outputs(outputs, geographic_frame, budget)?;
                writer.finish_geometry(output_frame.clone())
            },
        )?;
        if finished.len() != 1 {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "scalar Voronoi publication did not produce one finished array",
            ));
        }
        return Ok(crate::PyGeometryArray::from_finished(
            finished.pop().ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err("finished Voronoi array is absent")
            })?,
        )?);
    }
    let cells = voronoi_outputs(
        py,
        shapes,
        &frame,
        geographic_frame,
        tolerance,
        clip,
        operation,
        kernel,
        move |outputs, _, budget| -> Result<crate::geometry::PublishedVoronoiPolygonOutputs> {
            crate::geometry::publish_voronoi_polygon_outputs(outputs, geographic_frame, budget)
        },
    )?;
    let mut receipts = Vec::with_capacity(cells.len());
    let mut shapes = Vec::new();
    for outputs in cells {
        let (mut row, receipt): (Vec<Shape>, crate::geometry::VoronoiReceiptHold) =
            outputs.into_parts();
        shapes.append(&mut row);
        receipts.push(receipt);
    }
    let output = PyGeometryArray::from_shapes(shapes, frame)?;
    drop(receipts);
    Ok(output)
}

/// Voronoi edges straight into packed line storage: the kernel hands back one
/// XY column per input shape and the columns concatenate once.
pub(crate) fn voronoi_edge_lines_flatten(
    py: Python<'_>,
    shapes: impl Iterator<Item = Arc<crate::geometry::ShapeData>>,
    frame: Frame,
    tolerance: f64,
    clip: &VoronoiClipInput,
) -> PyResult<PyGeometryArray> {
    let geographic_frame = crate::geometry::topology_domain(&frame)
        .ok()
        .and_then(|domain| match domain {
            crate::geometry::TopologyDomain::Geographic(frame) => Some(frame),
            crate::geometry::TopologyDomain::Cartesian => None,
        });
    if let Some(geographic_frame) = geographic_frame {
        let output_frame = frame.clone();
        let mut finished = voronoi_outputs(
            py,
            shapes,
            &frame,
            Some(geographic_frame),
            tolerance,
            clip,
            "voronoi_edge_lines",
            Shape::voronoi_edge_column_in_frame_budgeted,
            move |output, _boundary, budget| {
                let mut writer = PublicationRows::with_producer_drawing_capacity(0);
                writer.push_voronoi_edge_column(output, geographic_frame, budget)?;
                writer.finish_geometry(output_frame.clone())
            },
        )?;
        if finished.len() != 1 {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "scalar Voronoi edge publication did not produce one finished array",
            ));
        }
        return Ok(crate::PyGeometryArray::from_finished(
            finished.pop().ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err("finished Voronoi edge array is absent")
            })?,
        )?);
    }
    let columns = voronoi_outputs(
        py,
        shapes,
        &frame,
        geographic_frame,
        tolerance,
        clip,
        "voronoi_edge_lines",
        Shape::voronoi_edge_column_in_frame_budgeted,
        move |output, _, budget| -> Result<crate::geometry::PublishedVoronoiEdgeColumn> {
            crate::geometry::publish_voronoi_edge_column_with_budget(
                output,
                geographic_frame,
                budget,
            )
        },
    )?;
    let mut receipts = Vec::with_capacity(columns.len());
    let packed_columns = columns
        .into_iter()
        .map(|column| {
            let (coordinates, offsets, receipt): (
                crate::geometry::CoordSeq,
                crate::geometry::CsrOffsetColumn,
                crate::geometry::VoronoiReceiptHold,
            ) = column.into_parts();
            receipts.push(receipt);
            (coordinates, offsets)
        })
        .collect();
    let (coords, offsets) = concat_packed_line_columns(packed_columns)?;
    let output = if coords.is_empty() {
        PyGeometryArray::from_shapes(Vec::new(), frame)?
    } else {
        PyGeometryArray::packed_lines(coords, offsets, frame)?
    };
    drop(receipts);
    Ok(output)
}

pub(crate) fn py_polygonize_full(
    py: Python<'_>,
    full: PolygonizeFull,
    crs: Option<&Crs>,
    epoch: Option<f64>,
    missing: Option<&ValidityMask>,
) -> PyResult<Py<PyAny>> {
    let frame = match Frame::new(crs.cloned(), epoch) {
        Ok(frame) => frame,
        Err(error) => {
            drop(full);
            return Err(error.into());
        },
    };
    let result_type = polygonize_result_type(py)?;
    let parts = Groups::polygonize_full_parts(full, &frame, missing)?;
    let result = result_type.call1(parts)?;
    Ok(result.unbind())
}
