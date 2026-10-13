use super::{
    Bound, Frame, FrameAdoption, PyAny, PyGeometry, PyResult, Shape, metadata, parse_crs_epoch,
};

pub(super) fn geometry_with_crs_epoch(
    shape: impl crate::geometry::IntoSharedShape,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyGeometry> {
    Ok(PyGeometry::with_frame(shape, parse_crs_epoch(crs, epoch)?))
}

pub(super) fn geometry_with_frame(
    shape: Shape,
    row_frame: Frame,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyGeometry> {
    let explicit = parse_crs_epoch(crs, epoch)?;
    let mut frames = [row_frame];
    let frame = metadata::resolve_frame_rows(
        &mut frames,
        FrameAdoption {
            crs: explicit.crs_owned(),
            epoch: explicit.epoch(),
        },
        "Point",
    )?;
    Ok(PyGeometry::with_frame(shape, frame))
}
