//! Frame admission for collections of Python geometry objects.

use crate::PyGeometry;
use crate::crs::frame::{Frame, FrameAdoption, FrameError, crs_operationally_equal, epochs_equal};
use crate::error::Result;

/// Shared frame: every item must agree on CRS and coordinate epoch (all
/// absent, or all naming the same frame). Genuinely mixed frames are
/// rejected rather than silently coerced. Empty input carries no metadata.
/// The single frame a collection, array, index, or reduction operates in.
///
/// CRS agreement is [`crs_operationally_equal`], so `EPSG:4326` and
/// `OGC:CRS84` items combine and the **first** item's label is the stored
/// one — the same left-biased rule binary results already use. That is not
/// a silent retag: the comparator admits a pair only when the coordinates
/// mean the same thing under either label, and it rejects every genuine
/// difference (datum, deriving conversion, units, axis direction,
/// dimension).
pub(crate) fn common_frame(items: &[PyGeometry], context: &str) -> Result<Frame> {
    let Some(first) = items.first() else {
        return Ok(Frame::None);
    };
    for (index, item) in items.iter().enumerate().skip(1) {
        let agrees = match (first.crs_ref(), item.crs_ref()) {
            (None, None) => true,
            (Some(left), Some(right)) => crs_operationally_equal(left, right)?,
            _ => false,
        };
        if !agrees {
            return Err(FrameError::SharedCrs {
                context: context.into(),
                index,
                first: first.crs_ref().cloned(),
                other: item.crs_ref().cloned(),
            }
            .into());
        }
        if item.epoch() != first.epoch() {
            return Err(FrameError::SharedEpoch {
                context: context.into(),
                index,
                first: first.epoch(),
                other: item.epoch(),
            }
            .into());
        }
    }
    // Every item's frame already upholds `epoch ⟹ crs`, so the shared
    // frame does too — `first`'s frame IS the answer once agreement holds.
    Ok(first.frame.clone())
}

pub(crate) trait FrameCarrier {
    fn frame(&self) -> &Frame;
    fn set_frame(&mut self, frame: Frame);
}

impl FrameCarrier for Frame {
    fn frame(&self) -> &Frame {
        self
    }

    fn set_frame(&mut self, frame: Frame) {
        *self = frame;
    }
}

impl FrameCarrier for PyGeometry {
    fn frame(&self) -> &Frame {
        &self.frame
    }

    fn set_frame(&mut self, frame: Frame) {
        self.frame = frame;
    }
}

/// Resolve shared frame metadata for geometry owners or separate frame rows.
///
/// Coordinate factories retain only row frames while their output builder owns
/// the shapes, so the common admission rule does not require temporary
/// [`PyGeometry`] owners.
pub(crate) fn resolve_frame_rows<T: FrameCarrier>(
    rows: &mut [T],
    explicit: FrameAdoption,
    context: &str,
) -> Result<Frame> {
    let Some(first) = rows.first() else {
        return Frame::new(explicit.crs, explicit.epoch);
    };
    let first_frame = first.frame();
    let first_crs = first_frame.crs_ref().cloned();
    let first_epoch = first_frame.epoch();
    let crs = if let Some(crs) = explicit.crs {
        for (index, row) in rows.iter().enumerate() {
            let frame = row.frame();
            if let Some(existing) = frame.crs_ref()
                && !crs_operationally_equal(existing, &crs)?
            {
                return Err(FrameError::SharedCrs {
                    context: context.into(),
                    index,
                    first: Some(crs),
                    other: frame.crs_ref().cloned(),
                }
                .into());
            }
        }
        Some(crs)
    } else {
        for (index, row) in rows.iter().enumerate().skip(1) {
            let frame = row.frame();
            let agrees = match (first_crs.as_ref(), frame.crs_ref()) {
                (None, None) => true,
                (Some(left), Some(right)) => crs_operationally_equal(left, right)?,
                _ => false,
            };
            if !agrees {
                return Err(FrameError::SharedCrs {
                    context: context.into(),
                    index,
                    first: first_crs,
                    other: frame.crs_ref().cloned(),
                }
                .into());
            }
        }
        first_crs
    };
    let epoch = if let Some(epoch) = explicit.epoch {
        for (index, row) in rows.iter().enumerate() {
            let frame = row.frame();
            if frame
                .epoch()
                .is_some_and(|existing| !epochs_equal(existing, epoch))
            {
                return Err(FrameError::SharedEpoch {
                    context: context.into(),
                    index,
                    first: Some(epoch),
                    other: frame.epoch(),
                }
                .into());
            }
        }
        Some(epoch)
    } else {
        for (index, row) in rows.iter().enumerate().skip(1) {
            let frame = row.frame();
            if frame.epoch() != first_epoch {
                return Err(FrameError::SharedEpoch {
                    context: context.into(),
                    index,
                    first: first_epoch,
                    other: frame.epoch(),
                }
                .into());
            }
        }
        first_epoch
    };
    let frame = Frame::new(crs, epoch)?;
    for row in rows {
        row.set_frame(frame.clone());
    }
    Ok(frame)
}

/// Resolve the frame of user-supplied items after applying explicit
/// constructor metadata. Explicit `crs` labels CRS-free items but must
/// match already-tagged items; explicit `epoch` labels epoch-free items but
/// only when a final CRS exists. Missing explicit fields remain strict:
/// mixed present/absent metadata is rejected rather than inferred.
pub(crate) fn resolve_geometry_frames(
    items: &mut [PyGeometry],
    explicit: FrameAdoption,
    context: &str,
) -> Result<Frame> {
    resolve_frame_rows(items, explicit, context)
}
