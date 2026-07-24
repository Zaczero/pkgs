#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! CRS/epoch metadata helpers shared across every binary and array surface.
//!
//! [`Frame::common`] resolves the shared frame of a sequence, and the
//! `binary_output_*` and `ensure_*_compatible` helpers enforce the strict,
//! commutative frame-compatibility contract (CRS presence+value AND epoch) at
//! the boundary. The `crs_arc*` builders pack short CRS id strings inline and
//! share long definitions cheaply.
//! Re-exported at the crate root for `use super::*` / `crate::` consumers.

use smol_str::SmolStr;

use crate::crs::CrsError;
use crate::error::Result;
use crate::*;

/// Frame-compatibility errors: operands or collection items disagree on CRS
/// or coordinate-epoch metadata. Pairwise conflicts surface as
/// `gometry.CRSMismatchError`; a malformed single frame surfaces as
/// `gometry.CRSError`.
#[derive(Debug)]
pub(crate) enum FrameError {
    CrsMismatch {
        operation: Box<str>,
        left: Option<Crs>,
        right: Option<Crs>,
    },
    EpochMismatch {
        operation: Box<str>,
        left: Option<f64>,
        right: Option<f64>,
    },
    SharedCrs {
        context: Box<str>,
        index: usize,
        first: Option<Crs>,
        other: Option<Crs>,
    },
    SharedEpoch {
        context: Box<str>,
        index: usize,
        first: Option<f64>,
        other: Option<f64>,
    },
    EpochRequiresCrs,
    EpochRequiresDynamicCrs {
        crs: Crs,
    },
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CrsMismatch {
                operation,
                left,
                right,
            } => write!(
                formatter,
                "{operation} requires matching CRS metadata: both operands must share one CRS, \
                 or both be CRS-free; got {} and {}; use set_crs(...) or to_crs(...) \
                 to align them",
                crs_label(left.as_ref().map(Crs::as_str)),
                crs_label(right.as_ref().map(Crs::as_str)),
            ),
            Self::EpochMismatch {
                operation,
                left,
                right,
            } => write!(
                formatter,
                "{operation} requires matching coordinate epoch metadata: both operands must \
                 share one epoch, or both be epoch-free; got {} and {}",
                epoch_label(*left),
                epoch_label(*right),
            ),
            Self::SharedCrs {
                context,
                index,
                first,
                other,
            } => write!(
                formatter,
                "{context} requires one shared CRS; item 0 has {}, item {index} has {}",
                crs_label(first.as_ref().map(Crs::as_str)),
                crs_label(other.as_ref().map(Crs::as_str)),
            ),
            Self::SharedEpoch {
                context,
                index,
                first,
                other,
            } => write!(
                formatter,
                "{context} requires one shared coordinate epoch; item 0 has {}, \
                 item {index} has {}",
                epoch_label(*first),
                epoch_label(*other),
            ),
            Self::EpochRequiresCrs => write!(
                formatter,
                "a coordinate epoch requires a CRS; attach one with crs= (or set_crs(...)) \
                 before tagging an epoch"
            ),
            Self::EpochRequiresDynamicCrs { crs } => write!(
                formatter,
                "a coordinate epoch requires a dynamic CRS; {} is static. Remove epoch= or \
                 transform to a dynamic CRS first",
                crs.as_str(),
            ),
        }
    }
}

impl std::error::Error for FrameError {}

pub(crate) fn epoch_label(epoch: Option<f64>) -> String {
    epoch.map_or_else(|| "None".to_owned(), |value| value.to_string())
}

/// A geometry's coordinate reference frame: a CRS, optionally with a coordinate
/// epoch. Encodes the invariant that an epoch requires a CRS (a coordinate
/// epoch dates a *CRS* realization) — there is no epoch-without-CRS variant, so
/// the illegal state is unrepresentable rather than runtime-guarded (#37).
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) enum Frame {
    /// No CRS (and therefore no epoch) — CRS-free coordinates.
    #[default]
    None,
    /// A CRS with no coordinate epoch.
    Crs(Crs),
    /// A CRS with a coordinate epoch (dynamic-datum realization date).
    CrsEpoch(Crs, f64),
}

/// Explicit frame metadata supplied at a collection/array construction
/// boundary. Present fields may fill metadata-free items; conflicting tagged
/// items still fail.
#[derive(Clone, Debug, Default)]
pub(crate) struct FrameAdoption {
    pub(crate) crs: Option<Crs>,
    pub(crate) epoch: Option<f64>,
}

/// A metadata-only frame mutation at the Python setter boundary.
///
/// Centralizes the silent-retag guards and `epoch => crs` invariant for
/// `Geometry.set_crs`/`set_epoch` and their array counterparts.
#[derive(Clone, Debug)]
pub(crate) enum FrameEdit {
    SetCrs { crs: Option<Crs>, overwrite: bool },
    SetEpoch { epoch: Option<f64>, overwrite: bool },
}

/// Resolved metadata and transform options for `Geometry.to_crs` and
/// `GeometryArray.to_crs`.
#[derive(Clone, Debug)]
pub(crate) struct GeometryTransformFrame {
    pub(crate) source: Crs,
    pub(crate) target: Crs,
    pub(crate) output: Frame,
    pub(crate) options: crs::TransformOptions,
    pub(crate) identity: bool,
}

impl Frame {
    /// Build from optional parts, enforcing `epoch ⟹ dynamic crs` — the single
    /// checked constructor. Used at untrusted (Python / deserialization) ingress;
    /// internal propagation from valid parents uses
    /// [`Self::from_trusted_parts`].
    pub(crate) fn new(crs: Option<Crs>, epoch: Option<f64>) -> Result<Self> {
        match (crs, epoch) {
            (Some(crs), Some(epoch)) => {
                if !crs::is_dynamic(&crs)? {
                    return Err(FrameError::EpochRequiresDynamicCrs { crs }.into());
                }
                Ok(Self::CrsEpoch(crs, epoch))
            },
            (Some(crs), None) => Ok(Self::Crs(crs)),
            (None, None) => Ok(Self::None),
            (None, Some(_)) => Err(FrameError::EpochRequiresCrs.into()),
        }
    }

    /// Build from parts already known valid — internal propagation from a
    /// valid parent. Panics on a non-dynamic epoch frame (same invariant as
    /// [`Self::new`]); trusted callers must not pass incoherent pairs.
    pub(crate) fn from_trusted_parts(crs: Option<Crs>, epoch: Option<f64>) -> Self {
        Self::new(crs, epoch).expect("epoch requires a dynamic CRS")
    }

    /// The CRS as a borrow.
    pub(crate) const fn crs_ref(&self) -> Option<&Crs> {
        match self {
            Self::Crs(crs) | Self::CrsEpoch(crs, _) => Some(crs),
            Self::None => None,
        }
    }

    /// The CRS code as `&str`.
    pub(crate) fn crs_str(&self) -> Option<&str> {
        self.crs_ref().map(Crs::as_str)
    }

    /// The CRS as an owned `Option<Crs>` (clones — for the construction/return
    /// sites that need to thread the CRS onward).
    pub(crate) fn crs_owned(&self) -> Option<Crs> {
        self.crs_ref().cloned()
    }

    /// The coordinate epoch, if any.
    pub(crate) const fn epoch(&self) -> Option<f64> {
        match self {
            Self::CrsEpoch(_, epoch) => Some(*epoch),
            _ => None,
        }
    }

    /// Resolve the common output frame for a binary operation.
    ///
    /// Both operands must agree on CRS presence/value and coordinate epoch;
    /// mismatches are rejected before any coordinate operation runs.
    pub(crate) fn compatible(&self, other: &Self, operation: &str) -> Result<Self> {
        Self::compatible_parts(
            self.crs_ref(),
            self.epoch(),
            other.crs_ref(),
            other.epoch(),
            operation,
        )
    }

    /// Borrowed-parts form of [`Self::compatible`] for the remaining
    /// low-level broadcast/index helpers that already hold CRS/epoch fields
    /// separately. Keeps the compatibility rule and error messages owned by
    /// `Frame`.
    pub(crate) fn compatible_parts(
        left_crs: Option<&Crs>,
        left_epoch: Option<f64>,
        right_crs: Option<&Crs>,
        right_epoch: Option<f64>,
        operation: &str,
    ) -> Result<Self> {
        let crs = match (left_crs, right_crs) {
            (Some(left), Some(right)) if left == right => Some(left.clone()),
            (None, None) => None,
            (left, right) => {
                return Err(FrameError::CrsMismatch {
                    operation: operation.into(),
                    left: left.cloned(),
                    right: right.cloned(),
                }
                .into());
            },
        };
        let epoch = match (left_epoch, right_epoch) {
            (Some(left), Some(right)) if epochs_equal(left, right) => Some(left),
            (None, None) => None,
            (left, right) => {
                return Err(FrameError::EpochMismatch {
                    operation: operation.into(),
                    left,
                    right,
                }
                .into());
            },
        };
        Self::new(crs, epoch)
    }

    /// Strict shared frame: every item must agree on CRS (all absent or all
    /// equal) and coordinate epoch (all absent or all equal). Mixed frames are
    /// rejected rather than silently coerced. Empty input carries no metadata.
    /// The single frame a collection, array, index, or reduction operates in.
    pub(crate) fn common(items: &[PyGeometry], context: &str) -> Result<Self> {
        let Some(first) = items.first() else {
            return Ok(Self::None);
        };
        for (index, item) in items.iter().enumerate().skip(1) {
            if item.crs_ref() != first.crs_ref() {
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
    pub(crate) fn resolve_items(
        items: &mut [PyGeometry],
        explicit: FrameAdoption,
        context: &str,
    ) -> Result<Self> {
        if items.is_empty() {
            return Self::new(explicit.crs, explicit.epoch);
        }
        let first_crs = items[0].crs_ref().cloned();
        let first_epoch = items[0].epoch();
        let crs = if let Some(crs) = explicit.crs {
            for (index, item) in items.iter().enumerate() {
                if item.crs_ref().is_some_and(|existing| *existing != crs) {
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
                item.set_crs_keep_epoch(Some(crs.clone()));
            }
            Some(crs)
        } else {
            for (index, item) in items.iter().enumerate().skip(1) {
                if item.crs_ref() != first_crs.as_ref() {
                    return Err(FrameError::SharedCrs {
                        context: context.into(),
                        index,
                        first: first_crs,
                        other: item.crs_ref().cloned(),
                    }
                    .into());
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
            let frame = Self::new(crs, Some(epoch))?;
            for item in items.iter_mut() {
                item.set_epoch_keep_crs(Some(epoch));
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
        Self::new(crs, first_epoch)
    }

    /// Strict shared frame of already-validated outputs: every item must agree
    /// on CRS and coordinate epoch. Empty input carries no metadata.
    pub(crate) fn of_uniform(items: &[PyGeometry], context: &str) -> Result<Self> {
        Self::common(items, context)
    }
}

impl FrameEdit {
    pub(crate) fn apply(&self, frame: &Frame) -> Result<Frame> {
        match self {
            Self::SetCrs { crs, overwrite } => {
                guard_crs_retag(frame.crs_str(), crs.as_deref(), *overwrite)?;
                let epoch = if crs.is_none() { None } else { frame.epoch() };
                Frame::new(crs.clone(), epoch)
            },
            Self::SetEpoch { epoch, overwrite } => {
                let next = Frame::new(frame.crs_owned(), *epoch)?;
                guard_epoch_retag(frame.epoch(), *epoch, *overwrite)?;
                Ok(next)
            },
        }
    }
}

impl GeometryTransformFrame {
    pub(crate) fn new(
        source: &Frame,
        target: Crs,
        explicit_epoch: Option<f64>,
        mut options: crs::TransformOptions,
    ) -> Result<Self> {
        let source_crs = source.crs_owned().ok_or_else(|| {
            CrsError::message("cannot transform coordinates without source CRS metadata")
        })?;
        let same_crs = source_crs == target;
        // Dynamic-aware epoch policy: with no explicit `epoch=`, the source
        // epoch survives exactly while it still means something — the CRS is
        // unchanged, or the TARGET frame is dynamic (time-dependent
        // coordinates). A static target auto-clears it, so the result never
        // drags a meaningless epoch into strict frame checks against ordinary
        // epoch-free data in the same static CRS.
        let output_epoch = match explicit_epoch {
            Some(epoch) => Some(epoch),
            None if same_crs => source.epoch(),
            None => match source.epoch() {
                Some(epoch) if crs::is_dynamic(&target)? => Some(epoch),
                _ => None,
            },
        };
        let identity = same_crs && output_epoch == source.epoch();
        options.source_epoch = source.epoch();
        options.target_epoch = output_epoch;
        Ok(Self {
            source: source_crs,
            target: target.clone(),
            // `explicit_epoch` is user input, so this frame is untrusted ingress
            // and takes the checked constructor: an epoch aimed at a static
            // target must raise, never trip `from_trusted_parts`'s assertion.
            output: Frame::new(Some(target), output_epoch)?,
            options,
            identity,
        })
    }
}

/// `set_crs` declares what coordinates already mean — silently re-tagging one
/// declared CRS as another is almost always a reprojection mistake. Attaching
/// (``None`` -> crs), clearing (crs -> ``None``), and identical re-tags stay
/// free; a genuine relabel requires ``overwrite=True``.
fn guard_crs_retag(existing: Option<&str>, new: Option<&str>, overwrite: bool) -> Result<()> {
    if overwrite {
        return Ok(());
    }
    if let (Some(existing), Some(new)) = (existing, new)
        && existing != new
    {
        return Err(CrsError::message(format!(
            "set_crs would re-tag CRS {existing:?} as {new:?} without moving coordinates; \
             use to_crs to reproject, or pass overwrite=True to relabel deliberately"
        )));
    }
    Ok(())
}

/// The coordinate-epoch analogue of [`guard_crs_retag`]: changing a present
/// epoch to a different decimal year without moving coordinates is the same
/// silent-frame-change footgun as a CRS re-tag, so it requires
/// `overwrite=True`. Clearing (`new` is `None`) and setting a first epoch are
/// always allowed.
fn guard_epoch_retag(existing: Option<f64>, new: Option<f64>, overwrite: bool) -> Result<()> {
    if overwrite {
        return Ok(());
    }
    if let (Some(existing), Some(new)) = (existing, new)
        && !epochs_equal(existing, new)
    {
        return Err(CrsError::message(format!(
            "set_epoch would change the coordinate epoch from {existing} to {new} without \
             reprojecting; use to_crs to transform between epochs, or pass overwrite=True to \
             relabel deliberately"
        )));
    }
    Ok(())
}

/// Exact equality of two coordinate epochs. Epochs are canonicalized at every
/// ingress (`-0.0 → 0.0`, non-finite rejected), so bit-exact `==` is the
/// intended metadata comparison — a decimal year is a discrete label, not a
/// measurement, so a tolerance check would be wrong (hence the `float_cmp`
/// allow lives here once instead of at each call site).
#[expect(clippy::float_cmp, reason = "epochs are canonical discrete labels")]
pub(crate) fn epochs_equal(left: f64, right: f64) -> bool {
    left == right
}

pub(crate) fn crs_arc(value: impl Into<Crs>) -> Crs {
    value.into()
}

pub(crate) fn crs_arc_str(value: &str) -> Crs {
    SmolStr::new(value)
}

pub(crate) const fn crs_arc_static(value: &'static str) -> Crs {
    SmolStr::new_inline(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn new_rejects_epoch_without_crs() {
        let err = Frame::new(None, Some(2020.0)).unwrap_err();
        assert!(matches!(
            err.kind(),
            crate::error::ErrorKind::Frame(FrameError::EpochRequiresCrs)
        ));
    }

    #[test]
    fn compatible_resolves_matching_frame_and_rejects_mismatch() {
        let wgs84 = Frame::from_trusted_parts(Some(crs_arc_static("EPSG:4326")), Some(2020.0));
        let same = Frame::from_trusted_parts(Some(crs_arc_static("EPSG:4326")), Some(2020.0));
        let other = Frame::from_trusted_parts(Some(crs_arc_static("EPSG:3857")), Some(2020.0));

        let resolved = wgs84.compatible(&same, "union").unwrap();
        assert_eq!(resolved.crs_str(), Some("EPSG:4326"));
        assert_eq!(resolved.epoch(), Some(2020.0));

        let err = wgs84.compatible(&other, "union").unwrap_err();
        assert!(matches!(
            err.kind(),
            crate::error::ErrorKind::Frame(FrameError::CrsMismatch { .. })
        ));
    }

    #[test]
    fn frame_edit_clears_epoch_with_crs_and_guards_retags() {
        let frame = Frame::from_trusted_parts(Some(crs_arc_static("EPSG:4326")), Some(2020.0));

        let cleared = FrameEdit::SetCrs {
            crs: None,
            overwrite: false,
        }
        .apply(&frame)
        .unwrap();
        assert_eq!(cleared, Frame::None);

        let retag = FrameEdit::SetCrs {
            crs: Some(crs_arc_static("EPSG:3857")),
            overwrite: false,
        }
        .apply(&frame)
        .unwrap_err();
        assert!(matches!(
            retag.kind(),
            crate::error::ErrorKind::Crs(crate::crs::CrsError::Message(_))
        ));

        let epoch = FrameEdit::SetEpoch {
            epoch: Some(2021.0),
            overwrite: true,
        }
        .apply(&frame)
        .unwrap();
        assert_eq!(epoch.crs_str(), Some("EPSG:4326"));
        assert_eq!(epoch.epoch(), Some(2021.0));
    }

    #[test]
    fn transform_frame_resolves_epoch_defaults_and_identity() {
        let source = Frame::from_trusted_parts(Some(crs_arc_static("EPSG:4326")), Some(2020.0));
        let same = GeometryTransformFrame::new(
            &source,
            crs_arc_static("EPSG:4326"),
            None,
            crs::TransformOptions::default(),
        )
        .unwrap();
        assert!(same.identity);
        assert_eq!(same.output.epoch(), Some(2020.0));
        assert_eq!(same.options.source_epoch, Some(2020.0));
        assert_eq!(same.options.target_epoch, Some(2020.0));

        // Dynamic-aware policy: EPSG:3857 rides the WGS 84 ensemble (dynamic
        // realizations), so the source epoch stays meaningful and survives.
        let dynamic_target = GeometryTransformFrame::new(
            &source,
            crs_arc_static("EPSG:3857"),
            None,
            crs::TransformOptions::default(),
        )
        .unwrap();
        assert!(!dynamic_target.identity);
        assert_eq!(dynamic_target.output.crs_str(), Some("EPSG:3857"));
        assert_eq!(dynamic_target.output.epoch(), Some(2020.0));

        // A static plate-fixed target clears the epoch automatically.
        let static_target = GeometryTransformFrame::new(
            &source,
            crs_arc_static("EPSG:2180"),
            None,
            crs::TransformOptions::default(),
        )
        .unwrap();
        assert_eq!(static_target.output.crs_str(), Some("EPSG:2180"));
        assert_eq!(static_target.output.epoch(), None);

        let explicit = GeometryTransformFrame::new(
            &source,
            crs_arc_static("EPSG:3857"),
            Some(2025.0),
            crs::TransformOptions::default(),
        )
        .unwrap();
        assert_eq!(explicit.output.epoch(), Some(2025.0));
        assert_eq!(explicit.options.target_epoch, Some(2025.0));
    }
}
