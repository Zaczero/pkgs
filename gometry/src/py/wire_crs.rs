#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::{Crs, PyResult, crs_arc, io};

pub(crate) enum SharedRowCrs {
    Unseen,
    Shared(Option<Crs>),
}

impl SharedRowCrs {
    /// One shared CRS across the bulk rows: with an explicit ``crs=`` every
    /// embedded SRID already matched it (the conflict guard runs per row),
    /// and without one every row must agree with row 0 — the exact contract
    /// (and `SharedCrs` error) `Frame::resolve_items` enforced when these
    /// lanes staged per-row geometry handles.
    ///
    /// A CRS-free row against a shared frame established by an embedded SRID
    /// (or vice versa) is a mismatch when no explicit payload/argument CRS
    /// covers both — the silent stamp of an SRID onto plain rows is forbidden.
    pub(crate) fn admit(
        &mut self,
        row_crs: Option<Crs>,
        fallback: Option<&Crs>,
        row: usize,
        context: &str,
    ) -> PyResult<()> {
        match self {
            Self::Unseen => *self = Self::Shared(row_crs),
            Self::Shared(existing) if fallback.is_none() && *existing != row_crs => {
                return Err(crate::error::Error::from(
                    crate::boundary::metadata::FrameError::SharedCrs {
                        context: context.into(),
                        index: row,
                        first: existing.clone(),
                        other: row_crs,
                    },
                )
                .into());
            },
            Self::Shared(_) => {},
        }
        Ok(())
    }

    /// The frame the rows agreed on: an explicit ``crs=`` wins (embedded
    /// SRIDs already matched it), else whatever row 0 established.
    pub(crate) fn into_crs(self, fallback: Option<Crs>) -> Option<Crs> {
        match (fallback, self) {
            (Some(fallback), _) => Some(fallback),
            (None, Self::Shared(crs)) => crs,
            (None, Self::Unseen) => None,
        }
    }
}

/// Shared SRID agreement across present bulk rows.
enum SharedSrid {
    /// No present row admitted yet.
    Unseen,
    /// Every present row so far carries this normalized SRID (`None` = CRS-free).
    Agreed(Option<u32>),
}

/// Serialized-frame admission: accumulate raw `Option<u32>` SRIDs, resolve
/// each distinct code once, compare to explicit/storage CRS once, yield one
/// shared final [`Crs`]. Replaces per-row `SharedRowCrs` clone/compare on
/// Arrow WKB, BinaryView, bulk `from_wkb`, EWKT, and pickle.
pub(crate) struct SridFrameAdmission {
    /// Explicit caller `crs=` (wins the final frame when set).
    explicit: Option<Crs>,
    /// Arrow storage CRS (or other non-SRID payload frame), used as fallback
    /// when `explicit` is absent.
    storage: Option<Crs>,
    /// First present row's normalized SRID agreement.
    shared_srid: SharedSrid,
    /// One canonicalize per distinct nonzero code (finish + conflict messages).
    cache: io::SridCrsCache,
}

impl SridFrameAdmission {
    pub(crate) fn new(explicit: Option<Crs>, storage: Option<Crs>) -> Self {
        Self {
            explicit,
            storage,
            shared_srid: SharedSrid::Unseen,
            cache: io::SridCrsCache::default(),
        }
    }

    pub(crate) fn set_storage_crs(&mut self, storage: Option<Crs>) {
        if self.explicit.is_none() {
            self.storage = storage;
        }
    }

    /// Admit one present row's normalized embedded SRID (`None` = plain WKB).
    /// Null/missing rows must not call this (they establish no CRS).
    ///
    /// `source` is the conflict label (`"EWKB SRID"` / `"EWKT SRID"`).
    pub(crate) fn admit_srid(
        &mut self,
        srid: Option<u32>,
        row: usize,
        context: &str,
        source: &str,
    ) -> PyResult<()> {
        // Explicit/storage CRS covers mixed free/tagged rows; numeric agreement
        // is required only when neither fallback is set.
        let has_fallback = self.explicit.is_some() || self.storage.is_some();
        match &self.shared_srid {
            SharedSrid::Unseen => self.shared_srid = SharedSrid::Agreed(srid),
            SharedSrid::Agreed(existing) if !has_fallback && *existing != srid => {
                return self.srid_conflict(row, *existing, srid, context);
            },
            SharedSrid::Agreed(_) => {},
        }
        // Explicit crs= vs embedded: resolve this code once and compare strings.
        if let (Some(explicit), Some(code)) = (self.explicit.as_deref(), srid) {
            let embedded = self.cache.resolve(Some(code))?;
            guard_embedded_crs_conflict(embedded.as_deref(), Some(explicit), source)?;
        }
        Ok(())
    }

    /// Storage-metadata CRS vs embedded SRID (Arrow column CRS).
    pub(crate) fn guard_storage_srid(
        &mut self,
        srid: Option<u32>,
        storage_crs: Option<&str>,
        _row: usize,
    ) -> PyResult<()> {
        if let (Some(storage), Some(code)) = (storage_crs, srid) {
            let embedded = self.cache.resolve(Some(code))?;
            guard_embedded_crs_conflict(embedded.as_deref(), Some(storage), "EWKB SRID")?;
        }
        Ok(())
    }

    /// Finish: one shared final CRS. Explicit wins; else storage; else the
    /// resolved shared SRID (each distinct code resolved once via the cache).
    pub(crate) fn finish(mut self) -> PyResult<Option<Crs>> {
        if let Some(explicit) = self.explicit {
            return Ok(Some(explicit));
        }
        if let Some(storage) = self.storage {
            return Ok(Some(storage));
        }
        match self.shared_srid {
            SharedSrid::Unseen | SharedSrid::Agreed(None) => Ok(None),
            SharedSrid::Agreed(Some(code)) => {
                let crs = self.cache.resolve(Some(code))?;
                Ok(crs.map(crs_arc))
            },
        }
    }

    fn srid_conflict(
        &mut self,
        row: usize,
        first: Option<u32>,
        other: Option<u32>,
        context: &str,
    ) -> PyResult<()> {
        let first_crs = self.cache.resolve(first)?.map(crs_arc);
        let other_crs = self.cache.resolve(other)?.map(crs_arc);
        Err(
            crate::error::Error::from(crate::boundary::metadata::FrameError::SharedCrs {
                context: context.into(),
                index: row,
                first: first_crs,
                other: other_crs,
            })
            .into(),
        )
    }
}
pub(crate) fn guard_embedded_crs_conflict(
    embedded: Option<&str>,
    explicit: Option<&str>,
    source: &str,
) -> PyResult<()> {
    if let (Some(embedded), Some(explicit)) = (embedded, explicit)
        && embedded != explicit
        && !io::is_wire_alias_restore(explicit, embedded)
    {
        return Err(crate::py::errors::crs_mismatch_error(
            format!(
                "crs argument {explicit:?} conflicts with the embedded {source} {embedded:?}; \
             omit crs= or align them"
            ),
            Some(explicit),
            Some(embedded),
            None,
        ));
    }
    Ok(())
}

/// After a successful conflict guard, prefer an explicit wire-alias CRS when
/// the embedded SRID is exactly that alias's EPSG code (restores CRS84 after
/// an EWKB round-trip). Otherwise keep the embedded CRS.
pub(crate) fn prefer_wire_alias_crs(embedded: Option<Crs>, explicit: Option<&Crs>) -> Option<Crs> {
    match (embedded, explicit) {
        (Some(embedded), Some(explicit))
            if io::is_wire_alias_restore(explicit.as_str(), embedded.as_str()) =>
        {
            Some(explicit.clone())
        },
        (embedded, _) => embedded,
    }
}

/// Split an optional EWKT ``SRID=<code>;`` prefix from a WKT string,
/// returning the WKT body and the normalized embedded SRID.
///
/// `None` means no CRS (missing prefix or PostGIS SRID 0). Nonzero codes are
/// unresolved — callers resolve through [`io::crs_from_srid`] /
/// [`io::SridCrsCache`].
/// Does `text` carry the EWKT `SRID=` prefix?
///
/// The ONE rule [`split_ewkt_srid`] and `validation::is_wkt_string` genuinely
/// share. They are otherwise different jobs — a parser and a classifier — so
/// they are NOT merged; but this predicate was written out twice, and if the
/// prefix rule ever changed the classifier would silently stop recognizing
/// EWKT and `require` would misroute it to the GeoJSON decoder.
///
/// Case-insensitive and leading-whitespace tolerant, matching PostGIS.
pub(crate) fn has_ewkt_srid_prefix(text: &str) -> bool {
    text.trim_start()
        .get(..5)
        .is_some_and(|head| head.eq_ignore_ascii_case("SRID="))
}

pub(crate) fn split_ewkt_srid(text: &str) -> PyResult<(&str, Option<u32>)> {
    // The EWKT prefix is `SRID=<code>;` — recognized ASCII-case-insensitively
    // and after any leading whitespace, matching PostGIS's lenient acceptance.
    // A bare WKT body is returned verbatim (the WKT parser trims it itself).
    let trimmed = text.trim_start();
    if !has_ewkt_srid_prefix(text) {
        return Ok((text, None));
    }
    let rest = trimmed
        .get(5..)
        .ok_or_else(|| io::IoError::wkt("expected EWKT 'SRID=<code>;<wkt>'"))?;
    let Some((code, body)) = rest.split_once(';') else {
        return Err(io::IoError::wkt("expected EWKT 'SRID=<code>;<wkt>'").into());
    };
    let code: u32 = code
        .trim()
        .parse()
        .map_err(|_| io::IoError::wkt(format!("EWKT SRID {code:?}; expected an EPSG code")))?;
    // SRID 0 is PostGIS "unknown" → CRS-free (same normalize as EWKB).
    Ok((body, if code == 0 { None } else { Some(code) }))
}
