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
use crate::{Crs, PyGeometry, crs, crs_label};

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
    /// valid parent, which by contract already upheld `epoch ⟹ dynamic crs`.
    ///
    /// Constructs directly rather than re-running [`Self::new`]. That checked
    /// constructor consults PROJ (`crs::is_dynamic`), so routing trusted
    /// propagation through it had two costs: a `.expect()` turned any *PROJ*
    /// failure — an unavailable database after `crs_configure`, an exotic CRS —
    /// into a panic across the FFI boundary, reported as "epoch requires a
    /// dynamic CRS", which misdescribes the cause; and it paid a database
    /// lookup on every internal propagation site. The invariant is re-checked
    /// under `debug_assert!` only, as an own-bug tripwire.
    pub(crate) fn from_trusted_parts(crs: Option<Crs>, epoch: Option<f64>) -> Self {
        match (crs, epoch) {
            (Some(crs), Some(epoch)) => {
                // `unwrap_or(true)`: a PROJ lookup failure is not evidence of a
                // broken invariant, so it must not trip the assertion.
                debug_assert!(
                    crs::is_dynamic(&crs).unwrap_or(true),
                    "from_trusted_parts got an epoch on a static CRS: {crs}"
                );
                Self::CrsEpoch(crs, epoch)
            },
            (Some(crs), None) => Self::Crs(crs),
            (None, None) => Self::None,
            (None, Some(_)) => {
                debug_assert!(false, "from_trusted_parts got an epoch with no CRS");
                Self::None
            },
        }
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

    /// Resolve the common output frame for two operands.
    ///
    /// Presence must match (None/None ok; None/Some fails). Epochs keep the
    /// exact/presence rule. CRS labels may differ when they name the same
    /// frame under [`crs_operationally_equal`]; the **left** label is then
    /// preserved as output metadata — the coordinate result is commutative,
    /// the label is left-biased.
    ///
    /// This is the *only* frame-agreement rule. There used to be a second,
    /// stricter one for sites that select one stored label (array fill and
    /// replace, index insertion), on the grounds that admitting a differently
    /// spelled CRS would relabel the value. It does relabel it — but only ever
    /// between spellings of the same frame, because the comparator rejects
    /// every genuine difference. The practical effect of the split was that a
    /// `GeometryArray` could measure and compare a pair of geometries it then
    /// refused to hold, so it is gone.
    ///
    /// Structural identity is a different question and is *not* this: geometry
    /// `__eq__`/`__hash__`/`equals_identical` still compare stored labels
    /// exactly, because `CRS(4326) != CRS("OGC:CRS84")` as objects.
    pub(crate) fn compatible(&self, other: &Self, operation: &str) -> Result<Self> {
        Self::compatible_parts(
            self.crs_ref(),
            self.epoch(),
            other.crs_ref(),
            other.epoch(),
            operation,
        )
    }

    /// Borrowed-parts form of [`Self::compatible`] for low-level
    /// broadcast/index helpers that already hold CRS/epoch fields separately.
    pub(crate) fn compatible_parts(
        left_crs: Option<&Crs>,
        left_epoch: Option<f64>,
        right_crs: Option<&Crs>,
        right_epoch: Option<f64>,
        operation: &str,
    ) -> Result<Self> {
        let crs = match (left_crs, right_crs) {
            (None, None) => None,
            (Some(left), Some(right)) => {
                if crs_operationally_equal(left, right)? {
                    Some(left.clone())
                } else {
                    return Err(FrameError::CrsMismatch {
                        operation: operation.into(),
                        left: Some(left.clone()),
                        right: Some(right.clone()),
                    }
                    .into());
                }
            },
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
    /// dimension). Before this, an array constructor was the lone lane that
    /// refused a pair every predicate and metric already accepted.
    pub(crate) fn common(items: &[PyGeometry], context: &str) -> Result<Self> {
        let Some(first) = items.first() else {
            return Ok(Self::None);
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
            // An explicit `crs=` is the requested output label, so items only
            // have to *agree* with it (see `common`); every item is then
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
                item.set_crs_keep_epoch(Some(crs.clone()));
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
                        item.set_crs_keep_epoch(Some(label.clone()));
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

/// Do two CRS labels name the same frame for computation — string-equal, or
/// operationally equivalent under PROJ's axis-order-agnostic comparison?
///
/// The epoch sibling of this question is [`epochs_equal`]. The literal equality
/// fast path means the common case never reaches PROJ or its comparison cache.
///
/// This is the *only* CRS-agreement predicate: a hand-rolled attribute
/// checklist was proposed and killed by counterexample (it unified EPSG:2180
/// with EPSG:2177, identical on datum/ellipsoid/prime meridian/units/dimension
/// yet ~3000 km apart, because the deriving conversion is load-bearing). Never
/// reintroduce one.
pub(crate) fn crs_operationally_equal(left: &Crs, right: &Crs) -> Result<bool> {
    if left == right {
        return Ok(true);
    }
    crs::same(
        left.as_str(),
        right.as_str(),
        crs::CrsComparison::IgnoreAxisOrder,
    )
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

/// The WGS84 lon/lat label gometry stamps on geometry it originates itself:
/// grid cells and their coverages, decoded polylines, decoded `geojson`.
///
/// `OGC:CRS84` rather than `EPSG:4326` because it names the lon/lat axis order
/// these coordinates are actually stored in, and because it is what RFC 7946,
/// GeoParquet and GeoArrow all mandate.
///
/// It is a single constant because the two spellings are **not** interchangeable
/// as stored labels: `CRS(4326) != CRS("OGC:CRS84")` by design (object identity
/// is not operational compatibility), so a second spelling anywhere means two
/// parsers can produce arrays that refuse to concatenate. Route every stored
/// WGS84 label through here.
///
/// This is a *stored label*, not a computation reference frame. Code that
/// transforms **to** lon/lat for a geodesic calculation names its target
/// separately and must not use this.
pub(crate) const WGS84_LONLAT: &str = "OGC:CRS84";

/// [`WGS84_LONLAT`] as a ready-to-store [`Crs`].
pub(crate) const fn wgs84_crs() -> Crs {
    crs_arc_static(WGS84_LONLAT)
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
    fn compatible_accepts_axis_order_aliases_and_keeps_left_label() {
        let epsg = Frame::from_trusted_parts(Some(crs_arc_static("EPSG:4326")), None);
        let crs84 = Frame::from_trusted_parts(Some(crs_arc_static("OGC:CRS84")), None);

        let left_epsg = epsg.compatible(&crs84, "intersection").unwrap();
        assert_eq!(left_epsg.crs_str(), Some("EPSG:4326"));

        let left_crs84 = crs84.compatible(&epsg, "intersection").unwrap();
        assert_eq!(left_crs84.crs_str(), Some("OGC:CRS84"));
    }

    /// The property that makes one relaxed rule safe everywhere: agreement is
    /// PROJ's axis-order-agnostic equivalence, which admits a pair only when
    /// the coordinates mean the same thing under either label.
    ///
    /// EPSG:2180 and EPSG:2177 are the standing counterexample — identical on
    /// datum, ellipsoid, prime meridian, units and dimension, so an attribute
    /// checklist would unify them, yet the same raw coordinate lands ~3000 km
    /// apart because the deriving conversion differs. 3857/3395 likewise
    /// (~32.75 km in Y), and 4326/4258 differ by datum.
    #[test]
    fn compatible_rejects_same_attribute_different_conversion() {
        let frame =
            |code: &'static str| Frame::from_trusted_parts(Some(crs_arc_static(code)), None);
        for (left, right) in [
            ("EPSG:2180", "EPSG:2177"),
            ("EPSG:3857", "EPSG:3395"),
            ("EPSG:4326", "EPSG:4258"),
            ("EPSG:4326", "EPSG:4979"),
        ] {
            let Err(err) = frame(left).compatible(&frame(right), "intersection") else {
                panic!("{left} and {right} are different frames, yet resolved to one label");
            };
            assert!(
                matches!(
                    err.kind(),
                    crate::error::ErrorKind::Frame(FrameError::CrsMismatch { .. })
                ),
                "{left} vs {right} must fail as a CRS mismatch, got {err:?}"
            );
        }
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
