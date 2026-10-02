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
    let Some(first_item) = items.first() else {
        return Frame::new(explicit.crs, explicit.epoch);
    };
    let first_crs = first_item.crs_ref().cloned();
    let first_epoch = first_item.epoch();
    let crs = if let Some(crs) = explicit.crs {
        // An explicit `crs=` is the requested output label, so items only
        // have to *agree* with it (see `common_frame`); every item is then
        // retagged to the label the caller asked for.
        for (index, item) in items.iter().enumerate() {
            if let Some(existing) = item.crs_ref()
                && !crs_operationally_equal(existing, &crs)?
            {
                return Err(FrameError::SharedCrs {
                    context: context.into(),
                    index,
                    first: Some(crs),
                    other: item.crs_ref().cloned(),
                }
                .into());
            }
        }
        for item in items.iter_mut() {
            item.set_crs_keep_epoch(Some(crs.clone()))?;
        }
        Some(crs)
    } else {
        // No explicit label: the first item's label is the stored one, and
        // later items only have to name the same frame.
        for (index, item) in items.iter().enumerate().skip(1) {
            let agrees = match (first_crs.as_ref(), item.crs_ref()) {
                (None, None) => true,
                (Some(left), Some(right)) => crs_operationally_equal(left, right)?,
                _ => false,
            };
            if !agrees {
                return Err(FrameError::SharedCrs {
                    context: context.into(),
                    index,
                    first: first_crs,
                    other: item.crs_ref().cloned(),
                }
                .into());
            }
        }
        if let Some(label) = first_crs.as_ref() {
            for item in items.iter_mut() {
                if item.crs_ref().is_some_and(|existing| existing != label) {
                    item.set_crs_keep_epoch(Some(label.clone()))?;
                }
            }
        }
        first_crs
    };
    if let Some(epoch) = explicit.epoch {
        for (index, item) in items.iter().enumerate() {
            if item
                .epoch()
                .is_some_and(|existing| !epochs_equal(existing, epoch))
            {
                return Err(FrameError::SharedEpoch {
                    context: context.into(),
                    index,
                    first: Some(epoch),
                    other: item.epoch(),
                }
                .into());
            }
        }
        let frame = Frame::new(crs, Some(epoch))?;
        for item in items.iter_mut() {
            item.set_epoch_keep_crs(Some(epoch))?;
        }
        return Ok(frame);
    }
    for (index, item) in items.iter().enumerate().skip(1) {
        if item.epoch() != first_epoch {
            return Err(FrameError::SharedEpoch {
                context: context.into(),
                index,
                first: first_epoch,
                other: item.epoch(),
            }
            .into());
        }
    }
    Frame::new(crs, first_epoch)
}
